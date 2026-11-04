import base64
import socket
import ssl
import struct
import time
from concurrent.futures import ThreadPoolExecutor

import requests


class IPCAnalyticsUtils:
    """
    REST helpers for Internal-identity Password Check (IPC) tests against the
    Analytics service.

    Every request is a single fresh HTTPS connection (Connection: close), so a
    result can never ride on a TLS session or keep-alive stream that was
    authenticated earlier. Server verification is skipped for the same reason
    as IPCBase.mtls_request: the nodes keep their out-of-the-box certificates.
    """

    ANALYTICS_PORT = 8095
    ANALYTICS_SSL_PORT = 18095
    MGMT_SSL_PORT = 18091

    SERVICE_PATH = "/analytics/service"
    PROXY_PATH = "/_p/cbas/analytics/service"

    @staticmethod
    def _admin_auth(server):
        return server.rest_username, server.rest_password

    @classmethod
    def request(cls, host, path, port=None, cert=None, auth=None,
                method="GET", data=None, timeout=60):
        """
        One HTTPS request to `host`:`port` (default 18095).

        cert is a (cert_path, key_path) tuple or None. Leave auth as None to
        send no Authorization header, so that only the certificate can
        establish who is calling. Returns the requests.Response; raises
        requests.exceptions.SSLError if the connection is refused at the TLS
        layer.
        """
        url = f"https://{host}:{port or cls.ANALYTICS_SSL_PORT}{path}"
        return requests.request(
            method, url, cert=cert, auth=auth, data=data, verify=False,
            timeout=timeout, headers={"Connection": "close"},
        )

    @classmethod
    def query(cls, host, statement, port=None, cert=None, auth=None,
              proxied=False, timeout=120):
        """
        POST one statement to the Analytics query servlet.

        proxied=True goes through ns_server's /_p/cbas/ proxy on 18091
        instead of straight to the cbas port.
        """
        if proxied:
            path, port = cls.PROXY_PATH, port or cls.MGMT_SSL_PORT
        else:
            path = cls.SERVICE_PATH
        return cls.request(host, path, port=port, cert=cert, auth=auth,
                           method="POST", data={"statement": statement},
                           timeout=timeout)

    @classmethod
    def admin_query(cls, server, statement, timeout=120):
        """The same statement as Administrator over plain HTTP 8095, which
        no clientCertAuth state can refuse. Returns the requests.Response."""
        return requests.post(
            f"http://{server.ip}:{cls.ANALYTICS_PORT}{cls.SERVICE_PATH}",
            auth=cls._admin_auth(server), data={"statement": statement},
            timeout=timeout,
        )

    @classmethod
    def get_cluster(cls, server, timeout=30):
        """GET /analytics/cluster as Administrator over plain HTTP 8095.
        Returns the requests.Response."""
        return requests.get(
            f"http://{server.ip}:{cls.ANALYTICS_PORT}/analytics/cluster",
            auth=cls._admin_auth(server), timeout=timeout,
        )

    # The first three bytes of the credential header cbas peers send on the
    # inter-node ports when their certificate establishes no identity,
    # followed by a 4-byte big-endian length and an ASCII Authorization value.
    # The server answers with a 4-byte big-endian HTTP status.
    INTERNODE_MAGIC = b"\xba\x5e\x00"

    @staticmethod
    def internode_connect(host, port, cert=None, timeout=10):
        """TLS-connect to an Analytics inter-node port presenting `cert`
        ((cert_path, key_path) or None). Returns the SSLSocket."""
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        if cert:
            ctx.load_cert_chain(*cert)
        raw = socket.create_connection((host, port), timeout=timeout)
        try:
            return ctx.wrap_socket(raw, server_hostname=host)
        except Exception:
            raw.close()
            raise

    @classmethod
    def internode_probe(cls, host, port, cert=None, send=b"", wait=15,
                        reply_len=0):
        """
        Connect, send `send`, then read until the server closes or `wait`
        seconds pass.

        Returns a dict: outcome is 'tls_reject' (handshake refused),
        'closed' (server closed or reset the connection), 'reply' (it sent
        `reply_len` bytes, decoded as a big-endian int in 'status') or
        'open' (still connected after `wait`). 'elapsed' is seconds from
        the end of the handshake.
        """
        try:
            sock = cls.internode_connect(host, port, cert=cert)
        except (ssl.SSLError, ConnectionResetError, OSError) as exc:
            return {"outcome": "tls_reject", "elapsed": 0.0,
                    "error": str(exc)}
        start = time.time()
        try:
            if send:
                sock.sendall(send)
            sock.settimeout(wait)
            data = b""
            while True:
                chunk = sock.recv(64)
                if not chunk:
                    break
                data += chunk
                if reply_len and len(data) >= reply_len:
                    status = struct.unpack(">i", data[:4])[0]
                    return {"outcome": "reply", "status": status,
                            "elapsed": time.time() - start}
            return {"outcome": "closed", "elapsed": time.time() - start,
                    "data": data[:16]}
        except TimeoutError:
            return {"outcome": "open", "elapsed": time.time() - start}
        except (ssl.SSLError, ConnectionResetError, OSError) as exc:
            return {"outcome": "closed", "elapsed": time.time() - start,
                    "error": str(exc)}
        finally:
            sock.close()

    @classmethod
    def internode_credentials(cls, username, password):
        """The bytes a cbas peer sends to authenticate with credentials."""
        value = "Basic " + base64.b64encode(
            f"{username}:{password}".encode()).decode()
        return (cls.INTERNODE_MAGIC + struct.pack(">i", len(value))
                + value.encode("ascii"))

    @staticmethod
    def get_audit(server, timeout=30):
        """GET /settings/audit over plain HTTP 8091. Returns the Response."""
        return requests.get(
            f"http://{server.ip}:8091/settings/audit",
            auth=(server.rest_username, server.rest_password),
            timeout=timeout)

    @staticmethod
    def set_audit(server, enabled, disabled_ids, timeout=30):
        """POST /settings/audit over plain HTTP 8091: auditd on or off, with
        exactly `disabled_ids` disabled. Returns the Response."""
        return requests.post(
            f"http://{server.ip}:8091/settings/audit",
            auth=(server.rest_username, server.rest_password),
            data={"auditdEnabled": "true" if enabled else "false",
                  "disabled": ",".join(str(i) for i in disabled_ids)},
            timeout=timeout)

    @classmethod
    def link_request(cls, server, name, method="POST", params=None,
                     scope="Default", timeout=120):
        """POST/DELETE /analytics/link/<scope>/<name> as Administrator over
        plain HTTP 8095. Returns the Response."""
        return requests.request(
            method,
            f"http://{server.ip}:{cls.ANALYTICS_PORT}/analytics/link/"
            f"{scope}/{name}",
            auth=cls._admin_auth(server), data=params, timeout=timeout)

    @classmethod
    def internode_hold(cls, host, port, cert, count, timeout=15):
        """Open `count` TLS connections presenting `cert` at the same time
        and send nothing. Returns [(socket or None, seconds to finish the
        handshake)], None where the server refused the connection."""
        def one(_):
            start = time.time()
            try:
                sock = cls.internode_connect(host, port, cert=cert,
                                             timeout=timeout)
            except (ssl.SSLError, ConnectionResetError, OSError):
                sock = None
            return sock, time.time() - start

        with ThreadPoolExecutor(max_workers=count) as pool:
            return list(pool.map(one, range(count)))

    @staticmethod
    def socket_closed(sock):
        """True if the server has closed `sock` (or it was never open)."""
        if sock is None:
            return True
        sock.settimeout(0.2)
        try:
            return sock.recv(1) == b""
        except TimeoutError:
            return False
        except (ssl.SSLError, ConnectionResetError, OSError):
            return True

    @staticmethod
    def bucket_item_count(server, bucket, timeout=30):
        """KV itemCount of `bucket` from /pools/default/buckets (lags a few
        seconds behind writes)."""
        resp = requests.get(
            f"http://{server.ip}:8091/pools/default/buckets/{bucket}",
            auth=(server.rest_username, server.rest_password),
            timeout=timeout)
        resp.raise_for_status()
        return resp.json()["basicStats"]["itemCount"]
