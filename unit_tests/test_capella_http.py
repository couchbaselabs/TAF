"""
Unit tests for couchbase_utils/capella_utils/capella_http.py, run against
the real lib/capellaAPI APIRequests class and a local scripted HTTP
server (no Capella access needed).

Run with: jython unit_tests/test_capella_http.py
      or: python3 unit_tests/test_capella_http.py
"""
import json
import socket
import sys
import threading
import time
import unittest

sys.path = [".", "lib", "pytests", "couchbase_utils", "platform_utils",
            "connections", "constants"] + sys.path

try:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
    from SocketServer import ThreadingMixIn
except ImportError:
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from socketserver import ThreadingMixIn

from capellaAPI.capella.lib.APIRequests import APIRequests
from couchbase_utils.capella_utils import capella_http


class _Server(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class _Handler(BaseHTTPRequestHandler):
    # path -> list of (status, headers, body, delay); last entry repeats
    script = {}
    hits = {}
    lock = threading.Lock()

    def log_message(self, *args):
        pass

    def _serve(self):
        path = self.path.split("?")[0]
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        with _Handler.lock:
            n = _Handler.hits.get(path, 0)
            _Handler.hits[path] = n + 1
            steps = _Handler.script.get(path, [(200, {}, "{}", 0)])
            status, headers, body, delay = steps[min(n, len(steps) - 1)]
        if delay:
            time.sleep(delay)
        if status == 0:
            # Close the connection without sending a single byte, like a
            # load balancer dropping an idle keep-alive connection.
            self.close_connection = True
            self.wfile.flush()
            try:
                import socket
                self.connection.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            return
        body = body.encode("utf-8") if not isinstance(body, bytes) else body
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = _serve


class _FakeInput(object):
    def __init__(self, params):
        self.params = params

    def param(self, name, default):
        return self.params.get(name, default)


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class CapellaHTTPTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = _Server(("127.0.0.1", 0), _Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever)
        cls.thread.daemon = True
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        capella_http.uninstall(APIRequests)

    def setUp(self):
        _Handler.script = {}
        _Handler.hits = {}
        self.config = capella_http.Config(
            connect_timeout=2, read_timeout=2, connect_retries=2,
            read_retries=1, status_retries=2, backoff=0.01,
            cluster_busy_poll=0.01, cluster_busy_wait=5,
            max_429_attempts=3, max_429_wait=5, default_429_wait=1)
        capella_http.install(self.config, api_requests_cls=APIRequests)
        capella_http.set_rate_limit_handler(None)
        self.api = APIRequests("http://127.0.0.1:{}".format(self.port),
                               token="owner-token")

    def hits(self, path):
        return _Handler.hits.get(path, 0)

    def test_install_is_idempotent(self):
        before = APIRequests.api_get
        capella_http.install(self.config, api_requests_cls=APIRequests)
        self.assertEqual(before, APIRequests.api_get)
        self.assertFalse(hasattr(
            getattr(APIRequests.api_get, "_taf_orig", None), "_taf_orig"))

    def test_plain_success_one_call(self):
        resp = self.api.api_get("/v4/ok")
        self.assertEqual(200, resp.status_code)
        self.assertEqual(1, self.hits("/v4/ok"))

    def test_get_retried_on_503_then_succeeds(self):
        _Handler.script["/v4/flaky"] = [(503, {}, "busy", 0),
                                        (502, {}, "bad gw", 0),
                                        (200, {}, "{}", 0)]
        resp = self.api.api_get("/v4/flaky")
        self.assertEqual(200, resp.status_code)
        self.assertEqual(3, self.hits("/v4/flaky"))

    def test_get_returns_last_5xx_after_retries_without_raising(self):
        _Handler.script["/v4/down"] = [(503, {}, "down", 0)]
        resp = self.api.api_get("/v4/down")
        self.assertEqual(503, resp.status_code)
        self.assertEqual(1 + self.config.status_retries,
                         self.hits("/v4/down"))

    def test_post_not_retried_on_503(self):
        _Handler.script["/v4/create"] = [(503, {}, "busy", 0),
                                         (201, {}, "{}", 0)]
        resp = self.api.api_post("/v4/create", {"a": 1})
        self.assertEqual(503, resp.status_code)
        self.assertEqual(1, self.hits("/v4/create"))

    def test_500_on_write_not_retried(self):
        # Tests assert on 500s from writes (e.g. "already enabled").
        _Handler.script["/v4/err"] = [(500, {}, "boom", 0),
                                      (200, {}, "{}", 0)]
        resp = self.api.api_post("/v4/err", {})
        self.assertEqual(500, resp.status_code)
        self.assertEqual(1, self.hits("/v4/err"))

    def test_post_retried_once_when_dropped_before_response(self):
        _Handler.script["/v4/drop"] = [(0, {}, "", 0), (201, {}, "{}", 0)]
        resp = self.api.api_post("/v4/drop", {})
        self.assertEqual(201, resp.status_code)
        self.assertEqual(2, self.hits("/v4/drop"))

    def test_post_dropped_twice_is_reported(self):
        _Handler.script["/v4/drop2"] = [(0, {}, "", 0)]
        with self.assertRaises(capella_http.CapellaRequestError):
            self.api.api_post("/v4/drop2", {})
        self.assertEqual(2, self.hits("/v4/drop2"))

    def test_cluster_busy_422_waited_out(self):
        busy = ('{"code":422,"httpStatusCode":422,"message":"Temporarily '
                'unavailable while the Cluster is in the Scaling state."}')
        _Handler.script["/v4/busy"] = [(422, {}, busy, 0), (422, {}, busy, 0),
                                       (201, {}, "{}", 0)]
        resp = self.api.api_post("/v4/busy", {})
        self.assertEqual(201, resp.status_code)
        self.assertEqual(3, self.hits("/v4/busy"))

    def test_cluster_deployment_failed_422_returned(self):
        failed = ('{"code":422,"message":"Temporarily unavailable while the '
                  'Cluster is in the Deployment Failed state."}')
        _Handler.script["/v4/dead"] = [(422, {}, failed, 0),
                                       (201, {}, "{}", 0)]
        resp = self.api.api_get("/v4/dead")
        self.assertEqual(422, resp.status_code)
        self.assertEqual(1, self.hits("/v4/dead"))

    def test_cluster_turning_off_422_waited_out(self):
        # Another suite turning off its cluster in a shared project makes
        # project-level lists 422 for a few minutes (19878).
        busy = ('{"code":422,"message":"Temporarily unavailable while the '
                'Cluster is in the Turning Off state."}')
        _Handler.script["/v4/list"] = [(422, {}, busy, 0), (200, {}, "{}", 0)]
        resp = self.api.api_get("/v4/list")
        self.assertEqual(200, resp.status_code)
        self.assertEqual(2, self.hits("/v4/list"))

    def test_cluster_steady_state_422_returned(self):
        off = ('{"code":422,"message":"Temporarily unavailable while the '
               'Cluster is in the Turned Off state."}')
        _Handler.script["/v4/off"] = [(422, {}, off, 0), (200, {}, "{}", 0)]
        resp = self.api.api_get("/v4/off")
        self.assertEqual(422, resp.status_code)
        self.assertEqual(1, self.hits("/v4/off"))

    def test_cluster_busy_budget_shared_across_calls(self):
        busy = ('{"code":422,"message":"Temporarily unavailable while the '
                'Cluster is in the Rebalancing state."}')
        capella_http.install(capella_http.Config(
            cluster_busy_poll=0.125, cluster_busy_wait=0.25),
            api_requests_cls=APIRequests)
        _Handler.script["/v4/stuck"] = [(422, {}, busy, 0)] * 20
        self.assertEqual(422, self.api.api_get("/v4/stuck").status_code)
        first = self.hits("/v4/stuck")
        self.assertEqual(3, first)  # 2 waits of 0.125s use up the budget
        # A second call does not get a fresh budget.
        self.assertEqual(422, self.api.api_get("/v4/stuck").status_code)
        self.assertEqual(first + 1, self.hits("/v4/stuck"))

    def test_transient_500_on_get_retried(self):
        _Handler.script["/v4/get500"] = [(500, {}, "boom", 0),
                                         (200, {}, "{}", 0)]
        resp = self.api.api_get("/v4/get500")
        self.assertEqual(200, resp.status_code)
        self.assertEqual(2, self.hits("/v4/get500"))

    def test_persistent_500_on_get_returned(self):
        _Handler.script["/v4/always500"] = [(500, {}, "boom", 0)]
        resp = self.api.api_get("/v4/always500")
        self.assertEqual(500, resp.status_code)
        self.assertEqual(1 + self.config.idempotent_500_retries,
                         self.hits("/v4/always500"))

    def test_read_timeout_retried_for_get(self):
        _Handler.script["/v4/slow"] = [(200, {}, "{}", 3),
                                       (200, {}, "{}", 0)]
        start = time.time()
        resp = self.api.api_get("/v4/slow")
        self.assertEqual(200, resp.status_code)
        self.assertEqual(2, self.hits("/v4/slow"))
        self.assertLess(time.time() - start, 10)

    def test_read_timeout_bounded_when_server_hangs(self):
        _Handler.script["/v4/hang"] = [(200, {}, "{}", 6)]
        start = time.time()
        with self.assertRaises(capella_http.CapellaRequestError):
            self.api.api_get("/v4/hang")
        # 2 attempts x 2s read timeout + backoff, not 6s+ per attempt
        self.assertLess(time.time() - start, 6)

    def test_connection_refused_raises_after_retries(self):
        api = APIRequests("http://127.0.0.1:{}".format(_free_port()),
                          token="t")
        start = time.time()
        with self.assertRaises(capella_http.CapellaRequestError):
            api.api_get("/v4/nothing-listening")
        self.assertLess(time.time() - start, 10)

    def test_429_with_retry_after_then_success(self):
        _Handler.script["/v4/rl"] = [(429, {"Retry-After": "1"}, "{}", 0),
                                     (200, {}, "{}", 0)]
        start = time.time()
        resp = self.api.api_post("/v4/rl", {})
        self.assertEqual(200, resp.status_code)
        self.assertEqual(2, self.hits("/v4/rl"))
        self.assertGreaterEqual(time.time() - start, 0.9)

    def test_429_without_header_uses_body_window(self):
        _Handler.script["/v4/rlb"] = [
            (429, {}, json.dumps({"message": "Limit is 1 request per 1s"}), 0),
            (200, {}, "{}", 0)]
        resp = self.api.api_get("/v4/rlb")
        self.assertEqual(200, resp.status_code)
        self.assertEqual(2, self.hits("/v4/rlb"))

    def test_429_quota_returned_unchanged(self):
        _Handler.script["/v4/quota"] = [
            (429, {"Retry-After": "3600"}, "{}", 0), (200, {}, "{}", 0)]
        resp = self.api.api_post("/v4/quota", {})
        self.assertEqual(429, resp.status_code)
        self.assertEqual(1, self.hits("/v4/quota"))

    def test_429_gives_up_after_max_attempts(self):
        _Handler.script["/v4/always"] = [(429, {"Retry-After": "1"}, "{}", 0)]
        resp = self.api.api_get("/v4/always")
        self.assertEqual(429, resp.status_code)
        self.assertEqual(self.config.max_429_attempts,
                         self.hits("/v4/always"))

    def test_suspend_returns_429_immediately(self):
        _Handler.script["/v4/count"] = [(429, {"Retry-After": "1"}, "{}", 0),
                                        (200, {}, "{}", 0)]
        with capella_http.suspend_429_retries():
            resp = self.api.api_get("/v4/count")
        self.assertEqual(429, resp.status_code)
        self.assertEqual(1, self.hits("/v4/count"))
        # and the policy is back on afterwards
        resp = self.api.api_get("/v4/count")
        self.assertEqual(200, resp.status_code)

    def test_handler_used_for_owner_calls_only(self):
        calls = []

        def handler(api_obj, wait):
            calls.append((api_obj, wait))
            api_obj.bearer_token = "owner-token-2"
            return True

        capella_http.set_rate_limit_handler(handler)
        _Handler.script["/v4/h"] = [(429, {"Retry-After": "2"}, "{}", 0),
                                    (200, {}, "{}", 0)]
        start = time.time()
        resp = self.api.api_get("/v4/h")
        self.assertEqual(200, resp.status_code)
        self.assertEqual([(self.api, 2)], calls)
        self.assertLess(time.time() - start, 1.5)  # handler, no sleep

        _Handler.script["/v4/role"] = [(429, {"Retry-After": "1"}, "{}", 0),
                                       (200, {}, "{}", 0)]
        resp = self.api.api_get(
            "/v4/role", None, {"Authorization": "Bearer role-key"})
        self.assertEqual(200, resp.status_code)
        self.assertEqual(1, len(calls))  # explicit header: plain sleep

    def test_handler_failure_falls_back_to_sleep(self):
        def handler(api_obj, wait):
            raise RuntimeError("no other key")

        capella_http.set_rate_limit_handler(handler)
        _Handler.script["/v4/hf"] = [(429, {"Retry-After": "1"}, "{}", 0),
                                     (200, {}, "{}", 0)]
        resp = self.api.api_get("/v4/hf")
        self.assertEqual(200, resp.status_code)

    def test_clients_created_before_install_can_be_attached(self):
        capella_http.uninstall(APIRequests)
        old = APIRequests("http://127.0.0.1:{}".format(self.port), token="t")
        capella_http.install(self.config, api_requests_cls=APIRequests)
        capella_http.attach(old)
        _Handler.script["/v4/old"] = [(503, {}, "", 0), (200, {}, "{}", 0)]
        self.assertEqual(200, old.api_get("/v4/old").status_code)

    def test_retry_after_seconds(self):
        class R(object):
            def __init__(self, headers, content):
                self.headers, self.content = headers, content

        f = capella_http.retry_after_seconds
        self.assertEqual(7, f(R({"Retry-After": "7"}, b""), 60))
        self.assertEqual(60, f(R({"Retry-After": "soon"}, b""), 60))
        self.assertEqual(21, f(R({}, b"1 request(s) per 20s"), 60))
        self.assertEqual(61, f(R({}, b"5 requests per 60s"), 60))
        self.assertEqual(60, f(R({}, b"{}"), 60))
        self.assertEqual(60, f(R({}, None), 60))

    def test_config_from_params(self):
        cfg = capella_http.Config.from_params(_FakeInput({
            "capella_http_read_timeout": "45",
            "capella_http_retry_429": "False",
            "capella_http_backoff": "0.5"}))
        self.assertEqual(45, cfg.read_timeout)
        self.assertFalse(cfg.retry_429)
        self.assertEqual(0.5, cfg.backoff)
        self.assertEqual(30, cfg.connect_timeout)
        with self.assertRaises(TypeError):
            capella_http.Config(no_such_option=1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
