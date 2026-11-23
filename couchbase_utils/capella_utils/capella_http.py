# -*- coding: utf-8 -*-
"""
Resilience layer for the Capella REST clients in lib/capellaAPI.

lib/capellaAPI is a submodule we don't change from TAF, and its
APIRequests.api_get/api_post/api_put/api_patch/api_del send requests with
no timeout and no retry. A stalled connection then blocks a test until
the Jenkins job is killed, and a single 502 or connection reset fails a
test. Every V4 test also repeats its own one-shot 429 handling.

install() fixes that once per process, for every APIRequests instance
(including the per-role clients that tests create inline):

  * a default (connect, read) timeout on every request;
  * retries with exponential backoff for connection errors (any method,
    the request never reached the server), read errors and 502/503/504
    (idempotent methods only, so a POST is never sent twice);
  * one 429 policy: wait Retry-After (or the "N request(s) per Ns"
    window from the body, or a default) and retry; with a rate-limit
    handler registered, owner-key calls switch to the other owner key
    instead of sleeping. A 429 asking for a longer wait than
    max_429_wait (an hourly quota) is returned to the caller unchanged;
  * one log line per call: method, path, status, time, attempt and the
    request id to look up in the backend logs. 2xx bodies are not
    logged; error bodies are, truncated.

Retries never raise on a status: after the last attempt the caller gets
the last response, exactly as before, so tests asserting on 5xx/429
status codes still see them. suspend_429_retries() turns the 429 policy
off, for the rate-limit tests that count 429s.

Written for both Jython 2.7 (urllib3 1.25) and CPython 3.
"""
import logging
import re
import threading
import time
from contextlib import contextmanager

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REQUEST_ID_HEADER = "Vnd-Cloud-Couchbase-Com-Request-Id"
IDEMPOTENT_METHODS = frozenset(
    ["GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"])
_WRAPPED_METHODS = (("api_get", "GET"), ("api_post", "POST"),
                    ("api_put", "PUT"), ("api_patch", "PATCH"),
                    ("api_del", "DELETE"))
_RATE_WINDOW_RE = re.compile(
    r"(\d+)\s*requests?\s*(?:\(s\))?\s*per\s*(\d+)\s*s", re.IGNORECASE)
_MARKER = "_taf_capella_http_installed"
# 422 the control plane returns, without doing anything, while a cluster is
# scaling/rebalancing/turning off/... Only those in-progress ("...ing")
# states clear by themselves; "Deployment Failed" and "Turned Off" do not,
# so they are returned at once.
_CLUSTER_BUSY_RE = re.compile(
    r"Temporarily unavailable while the Cluster is in the (.+?) state",
    re.IGNORECASE)
_TRANSITIONAL_STATE_RE = re.compile(r"ing\b", re.IGNORECASE)
# Logger lib/capellaAPI writes every request path and full response body
# to at INFO.
LIB_LOGGER_NAME = "capellaAPI.capella.lib.APIRequests"


class CapellaRequestError(Exception):
    """A Capella API call that never got an HTTP response (connection
    refused/reset, timeout) after all retries."""


class Config(object):
    """Knobs; every one can be overridden with a test param of the same
    name prefixed with "capella_http_" (see from_params)."""

    DEFAULTS = dict(
        connect_timeout=30,      # seconds to open the TCP/TLS connection
        read_timeout=180,        # seconds to wait for the response
        connect_retries=3,
        read_retries=2,
        status_retries=3,
        backoff=1.0,             # 1s, 2s, 4s ... between retries
        retry_429=True,
        max_429_attempts=4,      # total attempts for a call that gets 429
        max_429_wait=120,        # longer Retry-After = quota, don't retry
        default_429_wait=60,     # when neither header nor body says
        log_lib_bodies=False,    # keep lib/capellaAPI's own INFO logging
        body_log_limit=600,      # chars of an error body to log
        idempotent_500_retries=2,  # GET/PUT that 500 are retried
        cluster_busy_wait=900,   # max s, per test process, to wait on 422
        cluster_busy_poll=30,    # "cluster is in the <...ing> state"
    )

    def __init__(self, **overrides):
        for key, value in self.DEFAULTS.items():
            setattr(self, key, overrides.pop(key, value))
        if overrides:
            raise TypeError("Unknown capella_http option(s): {}".format(
                sorted(overrides)))

    @classmethod
    def from_params(cls, input_obj):
        """Build from TestInput params (capella_http_<name>=<value>)."""
        overrides = dict()
        for key, default in cls.DEFAULTS.items():
            value = input_obj.param("capella_http_" + key, default)
            overrides[key] = _coerce(value, default)
        return cls(**overrides)


def _coerce(value, default):
    if isinstance(default, bool):
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("1", "true", "yes", "y")
    if isinstance(default, int):
        return int(float(value))
    if isinstance(default, float):
        return float(value)
    return value


class _State(object):
    def __init__(self):
        self.config = Config()
        self.log = logging.getLogger("test")
        self.rate_limit_handler = None
        self.suspend_count = 0
        # Seconds spent waiting on busy-cluster 422s in this process. One
        # budget for all calls, so a test that makes 36 calls against a
        # cluster that stays busy waits cluster_busy_wait once, not 36x.
        self.busy_wait_spent = 0.0
        self.lock = threading.Lock()


_state = _State()


def build_retry(config):
    """urllib3 Retry for the adapter: connection errors on any method,
    read errors and 502/503/504 on idempotent methods only. Never raises
    on a status, so the caller always gets the last response."""
    kwargs = dict(total=config.connect_retries + config.read_retries
                  + config.status_retries,
                  connect=config.connect_retries,
                  read=config.read_retries,
                  status=config.status_retries,
                  backoff_factor=config.backoff,
                  status_forcelist=(502, 503, 504),
                  raise_on_status=False,
                  # A 503 Retry-After is uncapped in urllib3 (could hang
                  # for an hour); plain backoff is enough for a 5xx.
                  respect_retry_after_header=False)
    try:
        # urllib3 >= 1.26
        return Retry(allowed_methods=IDEMPOTENT_METHODS, **kwargs)
    except TypeError:
        # urllib3 < 1.26 (Jython ships 1.25.10)
        return Retry(method_whitelist=IDEMPOTENT_METHODS, **kwargs)


class CapellaHTTPAdapter(HTTPAdapter):
    """HTTPAdapter with a default timeout and the retry policy above."""

    def __init__(self, config=None, **kwargs):
        self.capella_config = config or _state.config
        kwargs.setdefault("max_retries", build_retry(self.capella_config))
        kwargs.setdefault("pool_connections", 10)
        kwargs.setdefault("pool_maxsize", 20)
        super(CapellaHTTPAdapter, self).__init__(**kwargs)

    def send(self, request, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = (self.capella_config.connect_timeout,
                                 self.capella_config.read_timeout)
        return super(CapellaHTTPAdapter, self).send(request, **kwargs)


def mount(api_obj, config=None):
    """Mount the adapter on one APIRequests instance's session."""
    session = getattr(api_obj, "network_session", None)
    if session is None:
        return False
    adapter = CapellaHTTPAdapter(config or _state.config)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return True


def attach(*api_objs):
    """Mount on already-created clients and the APIRequests objects they
    hold (CapellaAPI keeps org_ops_apis / cluster_ops_apis)."""
    for obj in api_objs:
        if obj is None:
            continue
        mount(obj)
        for value in list(vars(obj).values()):
            if value is not obj and hasattr(value, "network_session") \
                    and hasattr(value, "api_get"):
                mount(value)


def retry_after_seconds(resp, default=None):
    """Seconds a 429 asks us to wait: Retry-After header, else the
    "N request(s) per Ns" window in the body, else default."""
    if default is None:
        default = _state.config.default_429_wait
    headers = getattr(resp, "headers", None) or {}
    value = headers.get("Retry-After")
    if value is not None:
        try:
            return max(1, int(float(value)))
        except (TypeError, ValueError):
            pass
    try:
        match = _RATE_WINDOW_RE.search(_text(resp))
        if match:
            return max(1, int(match.group(2)) + 1)
    except Exception:
        pass
    return default


def request_id(resp):
    headers = getattr(resp, "headers", None) or {}
    return headers.get(REQUEST_ID_HEADER)


def _text(resp):
    content = getattr(resp, "content", b"") or b""
    if isinstance(content, bytes):
        try:
            return content.decode("utf-8", "replace")
        except Exception:
            return repr(content)
    return str(content)


def _explicit_auth(args, kwargs):
    # Every api_* method takes headers as its third parameter.
    headers = kwargs.get("headers")
    if headers is None and len(args) > 1:
        headers = args[1]
    return isinstance(headers, dict) and "Authorization" in headers


def _retry_429_enabled():
    return _state.config.retry_429 and _state.suspend_count == 0


def _log_call(verb, endpoint, resp, elapsed, attempt):
    cfg = _state.config
    status = getattr(resp, "status_code", None)
    line = "[capella-http] {} {} -> {} ({:.2f}s){} request_id={}".format(
        verb, endpoint, status, elapsed,
        " attempt={}".format(attempt) if attempt > 1 else "",
        request_id(resp))
    if status is not None and status >= 400:
        body = _text(resp)
        if len(body) > cfg.body_log_limit:
            body = body[:cfg.body_log_limit] + "...<truncated>"
        line += " body={}".format(body)
    if status is not None and status >= 500:
        _state.log.warning(line)
    elif _state.suspend_count:
        # Rate-limit tests fire hundreds of calls; keep them out of INFO.
        _state.log.debug(line)
    else:
        _state.log.info(line)


_DROPPED_MARKERS = ("BadStatusLine(\"''\"", "BadStatusLine(''",
                    "No status line received", "RemoteDisconnected",
                    "Remote end closed connection")


def _dropped_before_response(error):
    text = str(error)
    return "Connection aborted" in text and any(
        m in text for m in _DROPPED_MARKERS)


def _wait_for_busy_cluster(verb, api_endpoint, state, attempt):
    """Sleep one poll interval if the cluster state clears by itself and
    this process still has busy-wait budget. Returns True if it slept
    (the caller should retry), False to hand the 422 to the test."""
    cfg = _state.config
    if "fail" in state.lower() or not _TRANSITIONAL_STATE_RE.search(state):
        return False
    with _state.lock:
        if _state.busy_wait_spent >= cfg.cluster_busy_wait:
            _state.log.warning(
                "[capella-http] {} {}: cluster still in the {} state and "
                "the {}s busy-wait budget is used up, returning the 422"
                .format(verb, api_endpoint, state, cfg.cluster_busy_wait))
            return False
        _state.busy_wait_spent += cfg.cluster_busy_poll
    _state.log.info(
        "[capella-http] {} {}: cluster is in the {} state, waiting {}s "
        "(attempt {})".format(verb, api_endpoint, state,
                              cfg.cluster_busy_poll, attempt + 1))
    time.sleep(cfg.cluster_busy_poll)
    return True


def _wrap(orig, verb):
    def wrapper(self, api_endpoint, *args, **kwargs):
        attempt = 0
        while True:
            attempt += 1
            start = time.time()
            try:
                resp = orig(self, api_endpoint, *args, **kwargs)
            except SystemExit as e:
                # lib/capellaAPI's CbcAPIError calls sys.exit(self), so a
                # connection error escapes every `except Exception` and
                # silently kills worker threads. Turn it back into an
                # ordinary exception that says what failed.
                if not isinstance(getattr(e, "code", None), Exception):
                    raise
                # The server closed a reused keep-alive connection without
                # sending a single byte (load balancer idle timeout): the
                # request never reached the API, so one retry is safe for
                # any method. Anything else is reported as before.
                if attempt == 1 and _dropped_before_response(e.code):
                    _state.log.warning(
                        "[capella-http] {} {} -> connection dropped before "
                        "any response ({}), retrying once".format(
                            verb, api_endpoint, e.code))
                    continue
                _state.log.warning(
                    "[capella-http] {} {} -> connection error after {:.2f}s "
                    "(retries exhausted): {}".format(
                        verb, api_endpoint, time.time() - start, e.code))
                raise CapellaRequestError("{} {} failed: {}".format(
                    verb, api_endpoint, e.code))
            except Exception as e:
                _state.log.warning(
                    "[capella-http] {} {} -> {} after {:.2f}s: {}".format(
                        verb, api_endpoint, type(e).__name__,
                        time.time() - start, e))
                raise
            _log_call(verb, api_endpoint, resp, time.time() - start, attempt)
            # GET and PUT are idempotent, so a transient 500 (backend
            # timeout, pod restart) is safe to repeat; a persistent 500
            # still reaches the test after the retries. POST/DELETE are
            # never repeated here (tests assert on 500s from those).
            if verb in ("GET", "PUT") and resp is not None \
                    and resp.status_code == 500 \
                    and attempt <= _state.config.idempotent_500_retries:
                wait = _state.config.backoff * (2 ** attempt)
                _state.log.info(
                    "[capella-http] 500 on {} {}, retrying in {:.0f}s "
                    "(attempt {})".format(verb, api_endpoint, wait,
                                          attempt + 1))
                time.sleep(wait)
                continue
            busy = _CLUSTER_BUSY_RE.search(_text(resp)) \
                if resp is not None and resp.status_code == 422 else None
            if busy and _wait_for_busy_cluster(verb, api_endpoint,
                                               busy.group(1), attempt):
                continue
            if resp is None or resp.status_code != 429 \
                    or not _retry_429_enabled() \
                    or attempt >= _state.config.max_429_attempts:
                return resp
            wait = retry_after_seconds(resp)
            if wait > _state.config.max_429_wait:
                _state.log.warning(
                    "[capella-http] {} {} -> 429 asks for {}s (quota, not a "
                    "rate window), returning it".format(
                        verb, api_endpoint, wait))
                return resp
            handled = False
            handler = _state.rate_limit_handler
            if handler is not None and not _explicit_auth(args, kwargs):
                try:
                    handled = bool(handler(self, wait))
                except Exception as e:
                    _state.log.warning(
                        "[capella-http] rate-limit handler failed: "
                        "{}".format(e))
            if not handled:
                _state.log.info(
                    "[capella-http] 429 on {} {}, waiting {}s before attempt "
                    "{}".format(verb, api_endpoint, wait, attempt + 1))
                time.sleep(wait)

    wrapper.__name__ = getattr(orig, "__name__", "api_call")
    wrapper.__doc__ = getattr(orig, "__doc__", None)
    wrapper._taf_orig = orig
    return wrapper


# _urllib_request (internal API: /sessions login, v2 keys, support logs)
# opens a fresh Session per call and returns None on any network error.
# Repeating is safe for reads and for the /sessions login (no side effect);
# other writes are returned as-is so nothing is ever created twice.
_URLLIB_SAFE_METHODS = frozenset(["GET", "HEAD", "OPTIONS", "PUT", "DELETE"])


def _is_safe_internal(method, url):
    method = (method or "GET").upper()
    return method in _URLLIB_SAFE_METHODS or (
        method == "POST" and str(url).rstrip("/").endswith("/sessions"))


def _wrap_urllib(orig):
    def wrapper(self, api, method="GET", *args, **kwargs):
        verb = (method or "GET").upper()
        path = re.sub(r"^https?://[^/]+", "", str(api))
        attempts = 1 + (_state.config.connect_retries
                        if _is_safe_internal(verb, api) else 0)
        resp = None
        for attempt in range(1, attempts + 1):
            start = time.time()
            resp = orig(self, api, method, *args, **kwargs)
            if resp is not None:
                _log_call(verb, path, resp, time.time() - start, attempt)
                return resp
            if attempt < attempts:
                wait = _state.config.backoff * (2 ** (attempt - 1))
                _state.log.warning(
                    "[capella-http] {} {} -> no response (network error), "
                    "retrying in {:.1f}s (attempt {})".format(
                        verb, path, wait, attempt + 1))
                time.sleep(wait)
        _state.log.warning("[capella-http] {} {} -> no response after {} "
                           "attempt(s)".format(verb, path, attempts))
        return resp

    wrapper.__name__ = getattr(orig, "__name__", "_urllib_request")
    wrapper._taf_orig = orig
    return wrapper


def install(config=None, logger=None, api_requests_cls=None):
    """Patch APIRequests once per process; later calls only update the
    config and logger. Clients created after this call get the adapter
    automatically; use attach() for clients created before it."""
    if api_requests_cls is None:
        from capellaAPI.capella.lib.APIRequests import APIRequests
        api_requests_cls = APIRequests
    with _state.lock:
        if config is not None:
            _state.config = config
            _state.busy_wait_spent = 0.0
        if logger is not None:
            _state.log = logger
        logging.getLogger(LIB_LOGGER_NAME).setLevel(
            logging.NOTSET if _state.config.log_lib_bodies
            else logging.WARNING)
        if getattr(api_requests_cls, _MARKER, False):
            return api_requests_cls
        orig_init = api_requests_cls.__init__

        def __init__(self, *args, **kwargs):
            orig_init(self, *args, **kwargs)
            mount(self)

        __init__._taf_orig = orig_init
        api_requests_cls.__init__ = __init__
        for name, verb in _WRAPPED_METHODS:
            orig = getattr(api_requests_cls, name, None)
            if orig is not None:
                # In Jython/py2 getattr on the class returns an unbound
                # method; wrap the plain function underneath it.
                orig = getattr(orig, "__func__", orig)
                setattr(api_requests_cls, name, _wrap(orig, verb))
        orig = getattr(api_requests_cls, "_urllib_request", None)
        if orig is not None:
            orig = getattr(orig, "__func__", orig)
            setattr(api_requests_cls, "_urllib_request", _wrap_urllib(orig))
        setattr(api_requests_cls, _MARKER, True)
    return api_requests_cls


def uninstall(api_requests_cls=None):
    """Undo install() (used by the unit tests)."""
    if api_requests_cls is None:
        from capellaAPI.capella.lib.APIRequests import APIRequests
        api_requests_cls = APIRequests
    with _state.lock:
        if not getattr(api_requests_cls, _MARKER, False):
            return
        init = api_requests_cls.__dict__["__init__"]
        api_requests_cls.__init__ = getattr(init, "_taf_orig", init)
        for name in [m for m, _ in _WRAPPED_METHODS] + ["_urllib_request"]:
            func = api_requests_cls.__dict__.get(name)
            if func is not None and hasattr(func, "_taf_orig"):
                setattr(api_requests_cls, name, func._taf_orig)
        setattr(api_requests_cls, _MARKER, False)


def set_rate_limit_handler(handler):
    """handler(api_obj, wait_seconds) -> True if it dealt with the wait
    (e.g. switched to an unblocked key), False to make us sleep."""
    _state.rate_limit_handler = handler


@contextmanager
def suspend_429_retries():
    """Hand 429s straight back to the caller (rate-limit tests)."""
    with _state.lock:
        _state.suspend_count += 1
    try:
        yield
    finally:
        with _state.lock:
            _state.suspend_count -= 1
