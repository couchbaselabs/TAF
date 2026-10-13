import requests


class IPCUtils:
    """
    REST helpers for Internal-identity Password Check (IPC) tests.

    Calls go over plain HTTP 8091: the framework's HTTPS REST carries no
    client certificate, so it is refused while clientCertAuth is
    'mandatory'.
    """

    MGMT_PORT = 8091

    @classmethod
    def _url(cls, server, path):
        return f"http://{server.ip}:{cls.MGMT_PORT}{path}"

    @staticmethod
    def _admin_auth(server):
        return server.rest_username, server.rest_password

    @classmethod
    def set_user_locked(cls, server, username, locked, timeout=30):
        """
        PATCH /settings/rbac/users/local/<username> with locked=true|false.

        Returns the requests.Response.
        """
        return requests.patch(
            cls._url(server, f"/settings/rbac/users/local/{username}"),
            auth=cls._admin_auth(server),
            data={"locked": "true" if locked else "false"},
            timeout=timeout,
        )

    @classmethod
    def extract_user_from_cert(cls, server, cert_der, timeout=30):
        """
        POST /_cbauth/extractUserFromCert with the raw DER certificate.

        A certificate that names nobody still returns HTTP 200, with an
        empty object. Returns the requests.Response.
        """
        return requests.post(
            cls._url(server, "/_cbauth/extractUserFromCert"),
            auth=cls._admin_auth(server),
            data=cert_der,
            headers={"Content-Type": "application/octet-stream"},
            timeout=timeout,
        )

    @classmethod
    def get_internal_settings(cls, server, auth=None, timeout=30):
        """
        GET /internalSettings. `auth` defaults to the server's admin
        credentials; pass () to send none. Returns the requests.Response.
        """
        return requests.get(
            cls._url(server, "/internalSettings"),
            auth=cls._admin_auth(server) if auth is None else auth,
            timeout=timeout,
        )

    @classmethod
    def post_internal_settings(cls, server, data, auth=None, timeout=30):
        """
        POST /internalSettings. `data` may be a dict or a raw form-encoded
        string -- the raw form is how a body with a duplicated key is sent.
        `auth` as for get_internal_settings. Returns the requests.Response.
        """
        return requests.post(
            cls._url(server, "/internalSettings"),
            auth=cls._admin_auth(server) if auth is None else auth,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=timeout,
        )

    @classmethod
    def add_node(cls, server, hostname, user=None, password=None,
                 client_cert_auth=None, services=None, v2=False,
                 extra=None, auth=None, timeout=180):
        """
        POST /controller/addNode (or addNodeV2) on cluster node `server`.

        `user`/`password` are the joining node's credentials and are only
        sent when given -- leaving both out is how a certificate-only
        addition is attempted. `extra` adds raw form fields, e.g. to probe
        parameter validation. `auth` is the caller's own credentials on
        `server`, defaulting to its admin. Returns the requests.Response.
        """
        data = {"hostname": hostname}
        if user is not None:
            data["user"] = user
        if password is not None:
            data["password"] = password
        if client_cert_auth is not None:
            data["clientCertAuth"] = "true" if client_cert_auth else "false"
        if services:
            data["services"] = services
        data.update(extra or {})
        path = "/controller/addNodeV2" if v2 else "/controller/addNode"
        return requests.post(
            cls._url(server, path),
            auth=cls._admin_auth(server) if auth is None else auth,
            data=data, timeout=timeout,
        )

    @classmethod
    def do_join_cluster(cls, joining_server, cluster_host, user=None,
                        password=None, client_cert_auth=None,
                        new_node_hostname=None, auth=None, timeout=180):
        """
        POST /node/controller/doJoinCluster on the JOINING node, asking it
        to join the cluster at `cluster_host`. `user`/`password` are the
        cluster's credentials. Returns the requests.Response.
        """
        data = {"hostname": cluster_host}
        if user is not None:
            data["user"] = user
        if password is not None:
            data["password"] = password
        if client_cert_auth is not None:
            data["clientCertAuth"] = "true" if client_cert_auth else "false"
        if new_node_hostname:
            data["newNodeHostname"] = new_node_hostname
        return requests.post(
            cls._url(joining_server, "/node/controller/doJoinCluster"),
            auth=cls._admin_auth(joining_server) if auth is None else auth,
            data=data, timeout=timeout,
        )

    @classmethod
    def eject_node(cls, server, otp_node, timeout=120):
        """POST /controller/ejectNode -- the way to drop a node that was
        added but never rebalanced in."""
        return requests.post(
            cls._url(server, "/controller/ejectNode"),
            auth=cls._admin_auth(server), data={"otpNode": otp_node},
            timeout=timeout,
        )

    @classmethod
    def set_client_cert_auth(cls, server, state, prefixes, auth=None,
                             timeout=30):
        """POST /settings/clientCertAuth on `server` (JSON body)."""
        return requests.post(
            cls._url(server, "/settings/clientCertAuth"),
            auth=cls._admin_auth(server) if auth is None else auth,
            json={"state": state, "prefixes": prefixes}, timeout=timeout,
        )

    @classmethod
    def create_local_user(cls, server, username, password, roles, auth=None,
                          timeout=30):
        """PUT /settings/rbac/users/local/<username>. Returns the
        requests.Response."""
        return requests.put(
            cls._url(server, f"/settings/rbac/users/local/{username}"),
            auth=cls._admin_auth(server) if auth is None else auth,
            data={"password": password, "roles": roles}, timeout=timeout,
        )

    @classmethod
    def hard_reset_node(cls, server, auth=None, timeout=120):
        """POST /controller/hardResetNode: returns the node to the
        uninitialised state. Returns the requests.Response."""
        return requests.post(
            cls._url(server, "/controller/hardResetNode"),
            auth=cls._admin_auth(server) if auth is None else auth,
            timeout=timeout,
        )

    @classmethod
    def rotate_internal_credentials(cls, server, timeout=300):
        """
        POST /node/controller/rotateInternalCredentials on `server`: a full
        rotation of that node's internal password. Blocks until the
        rotation, including its protection sleep, has finished. Returns the
        requests.Response.
        """
        return requests.post(
            cls._url(server, "/node/controller/rotateInternalCredentials"),
            auth=cls._admin_auth(server), timeout=timeout,
        )

    @classmethod
    def set_rotation_interval(cls, server, interval_ms, timeout=30):
        """POST /settings/security intCredsRotationInterval. Returns the
        requests.Response."""
        return requests.post(
            cls._url(server, "/settings/security"),
            auth=cls._admin_auth(server),
            data={"intCredsRotationInterval": interval_ms}, timeout=timeout,
        )
