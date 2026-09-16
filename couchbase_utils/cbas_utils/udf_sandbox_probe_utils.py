"""
Fixtures and host-side observation for Enterprise Analytics Python UDF
sandbox-isolation checks (test plan §9, IDEA-543).

The threat model is explicit in the plan: no seccomp, no landlock,
everything crossing the sandbox boundary is assumed hostile, and the
only two invariants are "cannot escape" and "cannot consume more
resources than allotted." Each check here is written as an attack, and
the assertion is about the *host*, not about whether the in-sandbox
call itself raised -- an in-sandbox process, file write, or socket is
expected to be possible and is not by itself a defect.

`SANDBOX_PROBE_MODULE_SOURCE` is a library fixture (built via
`udf_fixture_builder.build_pyz_fixture`) whose methods never let an
in-sandbox exception surface as an opaque SQL++ "Internal error" --
each wraps its attempt and reports {'ok': bool, 'result'|'error': ...},
so a test can tell "the attempt failed inside the sandbox" (expected,
fine) apart from "the call itself blew up" (masks the real signal).

`HostObserver` is the other half: SSH-based checks for whether an
attempt actually reached the host -- a listener never contacted, a
file never planted outside the sandbox root, no new host process.
"""

from global_vars import logger
from shell_util.remote_connection import RemoteMachineShellConnection

SANDBOX_PROBE_MODULE_SOURCE = (
    "import ctypes\n"
    "import os\n"
    "import socket\n"
    "import subprocess\n"
    "import urllib.request\n"
    "\n"
    "\n"
    "class SandboxProbe(object):\n"
    "    def _try(self, fn):\n"
    "        try:\n"
    "            return {'ok': True, 'result': repr(fn())}\n"
    "        except Exception as e:\n"
    "            return {'ok': False, 'error': '{}: {}'.format(type(e).__name__, e)}\n"
    "\n"
    "    def attempt_network(self, *args):\n"
    "        host = args[0] if len(args) > 0 else '127.0.0.1'\n"
    "        port = int(args[1]) if len(args) > 1 else 9797\n"
    "        nonce = args[2] if len(args) > 2 else 'nonce'\n"
    "        out = {}\n"
    "        out['raw_socket'] = self._try(\n"
    "            lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM))\n"
    "        out['dns'] = self._try(lambda: socket.gethostbyname('example.com'))\n"
    "        def _connect_and_send():\n"
    "            s = socket.create_connection((host, port), timeout=3)\n"
    "            try:\n"
    "                s.send(nonce.encode())\n"
    "            finally:\n"
    "                s.close()\n"
    "            return True\n"
    "        out['connect'] = self._try(_connect_and_send)\n"
    "        out['http'] = self._try(\n"
    "            lambda: urllib.request.urlopen(\n"
    "                'http://{}:{}/'.format(host, port), timeout=3).read())\n"
    "        return out\n"
    "\n"
    "    def attempt_write_outside_mount(self, *args):\n"
    "        path = args[0] if len(args) > 0 else '/opt/enterprise-analytics/pwned'\n"
    "        marker = args[1] if len(args) > 1 else 'pwned'\n"
    "        return self._try(lambda: open(path, 'w').write(marker))\n"
    "\n"
    "    def attempt_read_host_path(self, *args):\n"
    "        path = args[0] if len(args) > 0 else '/etc/shadow'\n"
    "        return self._try(lambda: repr(open(path, 'rb').read()[:200]))\n"
    "\n"
    "    def attempt_host_process(self, *args):\n"
    "        cmd = args[0] if len(args) > 0 else 'id'\n"
    "        out = {}\n"
    "        out['os_system'] = self._try(lambda: os.system(cmd))\n"
    "        out['subprocess'] = self._try(\n"
    "            lambda: subprocess.run(\n"
    "                cmd, shell=True, capture_output=True, timeout=5).stdout)\n"
    "        out['ctypes_libc'] = self._try(\n"
    "            lambda: ctypes.CDLL(None).system(cmd.encode()))\n"
    "        return out\n"
    "\n"
    "    def attempt_proc_access(self, *args):\n"
    "        out = {}\n"
    "        out['proc_net_tcp'] = self._try(lambda: open('/proc/net/tcp').read())\n"
    "        out['proc_1_root'] = self._try(lambda: os.listdir('/proc/1/root'))\n"
    "        out['proc_1_environ'] = self._try(\n"
    "            lambda: open('/proc/1/environ', 'rb').read())\n"
    "        return out\n"
    "\n"
    "    def attempt_traverse_root(self, *args):\n"
    "        # Inside a container '/..' resolves to '/' -- the sandbox's\n"
    "        # own root -- so listing it always succeeds and is not by\n"
    "        # itself an escape (verified live 2026-09-14: the listing\n"
    "        # only ever contains this container's own files). The\n"
    "        # decisive check is whether the traversal prefix can reach\n"
    "        # identifiable content that exists only on the real host,\n"
    "        # outside every bind mount -- e.g. a marker file planted\n"
    "        # directly on the host's own /tmp, which is not shared with\n"
    "        # this container.\n"
    "        target = args[0] if len(args) > 0 else 'nonexistent-marker'\n"
    "        out = {}\n"
    "        out['listdir'] = self._try(lambda: os.listdir('/../../../../../../'))\n"
    "        out['read_via_traversal'] = self._try(\n"
    "            lambda: open('/../../../../../../{}'.format(target), 'rb').read())\n"
    "        return out\n"
    "\n"
    "    def attempt_import_other_library(self, *args):\n"
    "        module_name = args[0] if len(args) > 0 else 'other_library_module'\n"
    "        return self._try(lambda: __import__(module_name))\n"
    "\n"
    "    def plant_marker(self, *args):\n"
    "        marker = args[0] if len(args) > 0 else 'marker'\n"
    "        out = {}\n"
    "        out['tmp'] = self._try(\n"
    "            lambda: open('/tmp/{}'.format(marker), 'w').write(marker))\n"
    "        out['udf_socket_dir'] = self._try(\n"
    "            lambda: open('/var/run/udf-sockets/{}'.format(marker), 'w')\n"
    "            .write(marker))\n"
    "        return out\n"
    "\n"
    "    def report_environment(self, *args):\n"
    "        return {\n"
    "            'PYTHON_DS_PATH': os.environ.get('PYTHON_DS_PATH'),\n"
    "            'sys_path_head': self._try(lambda: __import__('sys').path[:3]),\n"
    "            'kernel': self._try(lambda: os.uname().release),\n"
    "        }\n"
)


class HostObserver(object):
    """
    Host-side counterpart to `SANDBOX_PROBE_MODULE_SOURCE`: SSHes to one
    node to start a background listener, then check afterward whether
    it was ever contacted, and to check for files or processes a
    sandbox escape would have to leave outside the container.
    """

    def __init__(self, node):
        self.node = node
        self.log = logger.get("test")
        self.shell = RemoteMachineShellConnection(node)
        self._listener_marker = None
        self._listener_port = None

    def disconnect(self):
        self.shell.disconnect()

    def _listener_pattern(self, port):
        # Must exactly describe the real running command (including
        # -s <ip>, added in the same change that introduced this
        # method) and bracket the first character so pgrep/pkill's own
        # invocation -- which contains this same literal pattern text
        # on its command line -- can't match itself. Same technique as
        # process_running below. Verified live 2026-09-15 that without
        # this, pgrep always self-matched (reporting a listener
        # "present" with none running at all), and pkill always killed
        # its own invoking shell instead of the real nc loop -- so the
        # cleanup after it (rm -f the marker) never ran, a stale marker
        # from an earlier run survived, and the real nc process was
        # never actually stopped by stop_listener either.
        return f"[n]c -l -s {self.node.ip} -p {port}"

    def start_listener(self, port=9797):
        """
        Starts a background `nc` listener on the host (not in any
        container) that appends a line to a marker file per connection.
        :return: True if the listener process is confirmed running,
        False if `nc` isn't available on this node (report this rather
        than silently no-op -- a listener that never started would
        make "was_contacted() -> False" a false pass, not a real one).
        """
        has_nc, _ = self.shell.execute_command("command -v nc >/dev/null && echo present")
        if not has_nc or "present" not in has_nc[0]:
            self.log.error(f"nc is not available on {self.node.ip}; cannot run a host listener")
            return False

        self._listener_port = port
        self._listener_marker = f"/tmp/udf_sandbox_probe_listener_{port}.hit"
        self.shell.execute_command(
            f"pkill -f '{self._listener_pattern(port)}' 2>/dev/null; rm -f {self._listener_marker}")
        # No 2>&1: nc's own stderr (e.g. a stray connection reset) must
        # not be able to land in the marker file and be mistaken for a
        # real contact. Bind to this node's own address rather than
        # every interface, and verify by a nonce sent on the actual
        # connect attempt (was_contacted), not by "the file is
        # non-empty" -- verified live 2026-09-14 that an unrelated
        # source can otherwise leave a few stray bytes (e.g. a bare
        # "\n\n\n") that a mere non-empty check would misread as a real
        # contact.
        self.shell.execute_command(
            f"nohup sh -c 'while true; do nc -l -s {self.node.ip} -p {port} "
            f">> {self._listener_marker}; done' > /dev/null 2>&1 < /dev/null &"
        )
        running, _ = self.shell.execute_command(f"pgrep -f '{self._listener_pattern(port)}' && echo present")
        return bool(running and "present" in "\n".join(running))

    def was_contacted(self, nonce=None):
        """
        :param nonce: if given, only a listener hit containing this exact
        value counts -- see start_listener's note on why a bare
        non-empty check is not reliable. If omitted, falls back to the
        non-empty check for callers that don't have a nonce to check.
        """
        if not self._listener_marker:
            return False
        if nonce:
            out, _ = self.shell.execute_command(f"grep -qF -- {nonce} {self._listener_marker} && echo present")
        else:
            out, _ = self.shell.execute_command(f"test -s {self._listener_marker} && echo present")
        return bool(out and "present" in out[0])

    def stop_listener(self):
        if self._listener_port:
            self.shell.execute_command(
                f"pkill -f '{self._listener_pattern(self._listener_port)}' 2>/dev/null; true")

    def file_exists(self, path):
        out, _ = self.shell.execute_command(f"test -e {path} && echo present")
        return bool(out and "present" in out[0])

    def read_file(self, path):
        out, _ = self.shell.execute_command(f"cat {path} 2>/dev/null")
        return "\n".join(out) if out else None

    def write_file(self, path, content):
        """
        Writes `content` to `path` directly on the host (not in any
        container) -- used to plant a marker a sandboxed process should
        never be able to see, since it exists only on the host side of
        every bind mount.
        """
        self.shell.execute_command(f"printf '%s' '{content}' > {path}")

    def delete_file(self, path):
        self.shell.execute_command(f"rm -f {path}")

    def process_running(self, pattern):
        # Bracket the first character: pgrep -f searches the full argv of
        # every process, including its own -- an unbracketed pattern
        # matches pgrep's own invocation every time (verified live
        # 2026-09-13: pgrep -f 'anything_at_all' reports "present" even
        # when nothing else on the node references it). The bracketed
        # form's own argv contains "[x]est" rather than "xest", so it
        # can't self-match.
        bracketed = f"[{pattern[0]}]{pattern[1:]}" if pattern else pattern
        out, _ = self.shell.execute_command(f"pgrep -f '{bracketed}' && echo present")
        return bool(out and "present" in "\n".join(out))
