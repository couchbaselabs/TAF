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
