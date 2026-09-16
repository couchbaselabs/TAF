"""
Enterprise Analytics Python UDF library management.

The only way to upload, list, or remove a Python UDF library is a POST/GET/
DELETE against a privileged, per-node Unix domain socket
(`/opt/enterprise-analytics/var/<node-id>_ea_lib.sock`) -- there is no
remote REST surface. This client drives `curl --unix-socket` over SSH on
the target node, reusing `platform_utils/ssh_util` the same way the rest of
TAF drives node-local CLI tools.
"""

import base64

from global_vars import logger
from shell_util.remote_connection import RemoteMachineShellConnection

# Single source of truth for the executor container's name -- was
# duplicated as a literal in ensure_udf_executor_runtime below and in
# every test that inspects/kills it directly (cbas_python_udf.py).
UDF_EXECUTOR_CONTAINER_NAME = "enterprise-analytics-udf-executor"


class EAUDFLibraryClient:
    """
    Node-local Python UDF library client for one Enterprise Analytics node.
    """

    def __init__(self, node, username="Administrator", password="password"):
        """
        :param node: a TestInputServer (e.g. an entry from
        `cluster.cbas_nodes`) reachable over SSH.
        :param username/password: EA admin credentials -- required even
        over the privileged local socket.
        """
        self.node = node
        self.username = username
        self.password = password
        self.log = logger.get("test")
        self.shell = RemoteMachineShellConnection(node)

    def disconnect(self):
        self.shell.disconnect()

    def _find_socket(self):
        """
        Resolves the upload socket for *this* node's current cluster
        incarnation. A node re-initialized more than once (e.g. across
        repeated test runs) accumulates stale `<old-node-id>_ea_lib.sock`
        files from earlier incarnations that a plain glob would happily
        match -- and connecting to one of those fails with "connection
        refused" since nothing is listening on it any more. Matching the
        live node's actual UUID (`GET /nodes/self`, first 8 hex chars,
        per the automation handoff) avoids that.
        """
        cmd = f"curl -s -u {self.username}:{self.password} http://localhost:8091/nodes/self"
        output, _ = self.shell.execute_command(
            cmd + " | python3 -c \"import json,sys; print(json.load(sys.stdin)['nodeUUID'][:8])\""
        )
        if not output:
            self.log.error(f"Could not resolve the current node UUID on {self.node.ip}")
            return None
        node_id = output[0].strip()
        sock = f"/opt/enterprise-analytics/var/{node_id}_ea_lib.sock"
        exists, _ = self.shell.execute_command(f"test -S {sock} && echo present")
        if not exists or "present" not in exists[0]:
            self.log.error(f"Expected UDF library socket {sock} on {self.node.ip} does not exist")
            return None
        return sock

    def upload_library(self, scope, name, local_pyz_path, lib_type="python", username=None, password=None):
        """
        Uploads a library archive to this node.
        :return: (success, http_status, body) -- success is True only for
        HTTP 200; http_status/body reflect whatever curl actually observed
        so a caller can assert on a specific failure (401, 405, ...).
        """
        sock = self._find_socket()
        if sock is None:
            return False, None, None

        remote_path = f"/tmp/{local_pyz_path.rsplit('/', 1)[-1]}"
        if not self.shell.copy_file_local_to_remote(local_pyz_path, remote_path):
            self.log.error(f"Failed to copy {local_pyz_path} to {self.node.ip}:{remote_path}")
            return False, None, None

        user = username or self.username
        pwd = password or self.password
        cmd = (
            f"curl -s --unix-socket {sock} -u {user}:{pwd} -X POST "
            f'-F "type={lib_type}" -F "data=@{remote_path}" '
            f"http://localhost/api/v1/library/{scope}/{name} "
            '-w "\\n%{http_code}"'
        )
        self.log.info(f"Executing cmd on {self.node.ip} - \n{cmd}\n")
        output, error = self.shell.execute_command(cmd)
        self.shell.execute_command(f"rm -f {remote_path}")

        if not output:
            self.log.error(f"No output from upload curl: error={error}")
            return False, None, None

        http_status = output[-1].strip()
        body = "\n".join(output[:-1])
        success = http_status == "200"
        if not success:
            self.log.error(f"Library upload to {self.node.ip} returned {http_status}: {body}")
        return success, http_status, body

    def list_libraries(self, scope=None):
        """
        :return: (http_status, body). Known bug (unfiled, verified live
        2026-09-04): this endpoint always returns 200 with an empty array
        even when a library is installed and callable -- callers must
        assert library presence via Metadata.Library, not this listing.

        Also verified live (2026-09-06): the scope-qualified form
        (`GET /api/v1/library/{scope}`) 404s -- only the bare listing
        (`scope=None`) works.
        """
        sock = self._find_socket()
        if sock is None:
            return None, None
        path = f"/api/v1/library/{scope}" if scope else "/api/v1/library"
        cmd = (
            f"curl -s --unix-socket {sock} -u {self.username}:{self.password} "
            f'http://localhost{path} -w "\\n%{{http_code}}"'
        )
        output, _ = self.shell.execute_command(cmd)
        if not output:
            return None, None
        return output[-1].strip(), "\n".join(output[:-1])

    def drop_library(self, scope, name, username=None, password=None):
        """
        Attempts to remove a library over the same socket via DELETE.
        Whether library removal is a socket operation at all, or purely a
        SQL++ DDL concept (DROP LIBRARY), is an open question in the
        Python UDF test plan -- this method returns the raw observed
        (http_status, body) rather than asserting an expected outcome, so
        the calling test is what records the real behaviour.
        """
        sock = self._find_socket()
        if sock is None:
            return None, None
        user = username or self.username
        pwd = password or self.password
        cmd = (
            f"curl -s --unix-socket {sock} -u {user}:{pwd} -X DELETE "
            f"http://localhost/api/v1/library/{scope}/{name} "
            '-w "\\n%{http_code}"'
        )
        self.log.info(f"Executing cmd on {self.node.ip} - \n{cmd}\n")
        output, _ = self.shell.execute_command(cmd)
        if not output:
            return None, None
        return output[-1].strip(), "\n".join(output[:-1])

    def _cbas_data_dir(self):
        """
        Resolves this node's actual analytics storage directory via
        `GET /nodes/self` (`storage.hdd[0].cbas_dirs[0]`) rather than
        assuming a fixed path -- `cbas_path` is a configurable, possibly
        multi-valued ini/cluster setting (see TestInput.py's JSON parsing
        of a bracketed value), not always `/data`.
        """
        return _resolve_cbas_data_dir(
            self.shell, self.node, self.username, self.password, self.log)

    def artifact_path(self, database, scope, name):
        """
        The on-disk path of a library artifact on this node -- exposed
        publicly (not just used internally by artifact_present) since
        sandbox-isolation checks need to hand this exact path to a UDF
        running as a *different* library, to confirm it can't read it.
        """
        cbas_dir = self._cbas_data_dir()
        if cbas_dir is None:
            return None
        return (
            f"{cbas_dir}/@analytics/v_iodevice_0/applications/library/storage/"
            f"{database}/{scope}/{name}/library_archive.zip"
        )

    def artifact_present(self, database, scope, name, expected_md5=None):
        """
        Checks the on-disk library artifact on this node.
        :return: (exists, md5_or_none). When `expected_md5` is given, the
        method also verifies the on-disk content matches it.
        """
        remote_path = self.artifact_path(database, scope, name)
        if remote_path is None:
            return False, None
        output, _ = self.shell.execute_command(f"test -f {remote_path} && md5sum {remote_path}")
        if not output:
            return False, None
        md5 = output[0].split()[0].strip()
        if expected_md5 is not None and md5 != expected_md5:
            self.log.error(f"Artifact md5 mismatch on {self.node.ip}: expected {expected_md5}, got {md5}")
        return True, md5


def assert_library_on_every_node(
    cbas_nodes, database, scope, name, expected_md5=None, username="Administrator", password="password"
):
    """
    Checks the on-disk library artifact across every node in the cluster.
    `Metadata.Library` is a single cluster-wide catalog and can't tell
    whether distribution actually reached every node -- only the on-disk
    artifact can, so this is the per-node counterpart the distribution
    checks in the Library Upload/Lifecycle sections need.

    :return: dict of {node.ip: (exists, md5_or_none)}
    """
    results = {}
    for node in cbas_nodes:
        client = EAUDFLibraryClient(node, username, password)
        try:
            results[node.ip] = client.artifact_present(database, scope, name, expected_md5)
        finally:
            client.disconnect()
    return results


def _resolve_cbas_data_dir(shell, server, username, password, log):
    """
    Resolves a node's analytics storage directory via `GET /nodes/self`
    (`storage.hdd[0].cbas_dirs[0]`). `cbas_path` is a configurable,
    possibly multi-valued ini/cluster setting, so it cannot be assumed
    to be /data.

    Module-level so both EAUDFLibraryClient and the container
    provisioning below resolve it the same way.

    :return: the directory, or None if it could not be resolved.
    """
    output, _ = shell.execute_command(
        "curl -s -u {0}:{1} http://localhost:8091/nodes/self | "
        "python3 -c \"import json,sys; d=json.load(sys.stdin); "
        "print(d['storage']['hdd'][0]['cbas_dirs'][0])\"".format(
            username, password))
    if not output:
        log.error(
            "Could not resolve the analytics storage directory on "
            "{0}".format(server.ip))
        return None
    return output[0].strip()


def ensure_udf_executor_runtime(server, username="Administrator", password="password"):
    """
    Ensures the sandboxed Python UDF executor is actually running on
    this node.

    A fresh EA install has none of the pieces this needs, and none of
    them are provisioned automatically by the enterprise-analytics
    package or service:
    - the gVisor sandbox runtime (`runsc`) is not shipped at all;
    - Docker doesn't know about a "runsc" runtime until one is
      registered in /etc/docker/daemon.json;
    - the executor container itself
      (build-docker.couchbase.com/cb-vanilla/enterprise-analytics-udf,
      the same image `docker inspect` shows was used historically) is
      never pulled/started by anything running on the node -- the
      systemd unit ("enterprise-analytics-udf-appdir.service") that
      historically supervised it is created transiently under /run
      and does not survive a reboot, and nothing recreates it.

    Without all of this, every Python UDF call fails -- first with
    "Domain socket was not found at specified path" /
    java.net.ConnectException, regardless of storage backend.

    Note: /var/lib/docker/runtimes/ is recreated empty by Docker itself
    on every restart, so a wrapper script placed there does not
    survive -- the runtime has to be registered by path directly in
    daemon.json instead (verified live: a wrapper-script-based
    registration silently disappeared across a `systemctl restart
    docker`).

    Also note: the container must run as the *host's* `couchbase`
    uid:gid, not the image's own default user -- CBAS (running on the
    host as `couchbase`) connects to the socket the container creates,
    and a uid/gid mismatch there is a silent, host-specific trap: the
    image's baked-in user happens to map to a different, unrelated
    system account per host (verified live: uid 1000 on one node, 999
    on another), so connecting fails with `java.net.BindException:
    Permission denied` on whichever host doesn't happen to match --
    this cannot be hardcoded and must be resolved per node.

    Scoped to Python UDF tests only (called from CBASPythonUDF.setUp),
    not the general on-prem/EA setUp path -- no other EA feature needs
    this sandbox runtime, so there is no reason to pay this cost on
    every EA test. Safe to call repeatedly: each step is a no-op once
    already in place, and re-registering the same daemon.json content
    does not re-trigger a Docker restart.

    :return: True if the executor is up (already, or newly
    provisioned), False if provisioning failed.
    """
    log = logger.get("test")
    image = "build-docker.couchbase.com/cb-vanilla/enterprise-analytics-udf:2.3.0"
    container_name = UDF_EXECUTOR_CONTAINER_NAME
    socket_path = "/var/run/udf-sockets/pyudf.socket"
    runsc_path = "/usr/local/bin/runsc"
    daemon_json_path = "/etc/docker/daemon.json"

    shell = RemoteMachineShellConnection(server)
    try:
        couchbase_id_out, _ = shell.execute_command(
            "id -u couchbase && id -g couchbase")
        if not couchbase_id_out or len(couchbase_id_out) < 2:
            log.error(
                "Could not resolve the couchbase user's uid/gid on "
                "{0}".format(server.ip))
            return False
        couchbase_uid = couchbase_id_out[0].strip()
        couchbase_gid = couchbase_id_out[1].strip()

        # Resolved up front because the reuse check below needs it, not just
        # the `docker run` further down.
        cbas_dir = _resolve_cbas_data_dir(shell, server, username, password,
                                          log)
        if cbas_dir is None:
            return False
        applications_dir = "{0}/@analytics/v_iodevice_0/applications".format(
            cbas_dir)

        # A container is only reusable if it is running, its socket carries
        # the right ownership, *and* its bind mount still resolves to the
        # live applications directory. That last condition is not academic:
        # cluster init recreates the analytics data directory, which leaves
        # an already-running container bound to the old, now-unlinked inode
        # -- it sees an empty directory, the executor child exits instead of
        # completing the handshake, and every UDF call then fails with a
        # bare "ASX0201: External UDF returned exception" out of
        # PythonDomainSocketProto.helo(). Comparing inodes across the mount
        # catches that; ownership and liveness alone do not.
        running, _ = shell.execute_command(
            "docker inspect -f '{{.State.Running}}' " + container_name +
            " 2>/dev/null | grep -q true && "
            "stat -c '%u:%g' " + socket_path + " 2>/dev/null | "
            "grep -qx " + couchbase_uid + ":" + couchbase_gid + " && "
            "host_ino=$(stat -c %i " + applications_dir + " 2>/dev/null) && "
            "cont_ino=$(docker exec " + container_name + " stat -c %i " +
            applications_dir + " 2>/dev/null) && "
            '[ -n "$host_ino" ] && [ "$host_ino" = "$cont_ino" ]'
            " && echo present")
        if running and "present" in running[0]:
            log.info(
                "UDF executor container already running on {0}".format(
                    server.ip))
            return True

        # 1. The real gVisor binary (checksum-verified download).
        runsc_present, _ = shell.execute_command(
            "test -x {0} && echo present".format(runsc_path))
        if not runsc_present or "present" not in runsc_present[0]:
            arch_out, _ = shell.execute_command("uname -m")
            if not arch_out:
                log.error(
                    "Could not determine architecture on {0}".format(
                        server.ip))
                return False
            arch = arch_out[0].strip()

            base_url = (
                "https://storage.googleapis.com/gvisor/releases/release/"
                "latest/{0}".format(arch))
            shell.execute_command(
                "curl -fsSL -o /tmp/runsc.download {0}/runsc".format(
                    base_url))
            shell.execute_command(
                "curl -fsSL -o /tmp/runsc.download.sha512 "
                "{0}/runsc.sha512".format(base_url))

            verify_cmd = (
                "expected=$(awk '{print $1}' /tmp/runsc.download.sha512); "
                "actual=$(sha512sum /tmp/runsc.download | awk '{print $1}'); "
                "[ \"$expected\" = \"$actual\" ] && [ -n \"$expected\" ] "
                "&& echo checksum_ok"
            )
            verify_out, _ = shell.execute_command(verify_cmd)
            if not verify_out or "checksum_ok" not in verify_out[0]:
                log.error(
                    "gVisor runsc download checksum mismatch (or "
                    "download failed) on {0}".format(server.ip))
                shell.execute_command(
                    "rm -f /tmp/runsc.download /tmp/runsc.download.sha512")
                return False

            shell.execute_command(
                "chmod 0755 /tmp/runsc.download && "
                "mv /tmp/runsc.download {0} && "
                "rm -f /tmp/runsc.download.sha512".format(runsc_path))

        # 2. Register runsc as a named Docker runtime (merging into any
        # existing daemon.json rather than clobbering it), then restart
        # Docker only if it is not already registered.
        registered, _ = shell.execute_command(
            "docker info 2>/dev/null | tr ' ' '\\n' | grep -qx runsc && "
            "echo present")
        if not registered or "present" not in registered[0]:
            merge_script = (
                "import json, os\n"
                "path = '{0}'\n"
                "cfg = {{}}\n"
                "if os.path.exists(path):\n"
                "    with open(path) as f:\n"
                "        content = f.read().strip()\n"
                "    if content:\n"
                "        cfg = json.loads(content)\n"
                "runtimes = cfg.setdefault('runtimes', {{}})\n"
                "runtimes['runsc'] = {{\n"
                "    'path': '{1}',\n"
                "    'runtimeArgs': ['--network=none', '--host-uds=create', "
                "'--directfs=false']\n"
                "}}\n"
                "with open(path, 'w') as f:\n"
                "    json.dump(cfg, f, indent=2)\n"
            ).format(daemon_json_path, runsc_path)
            merge_b64 = base64.b64encode(
                merge_script.encode("utf-8")).decode("ascii")
            shell.execute_command(
                "echo '{0}' | base64 -d | python3".format(merge_b64))
            shell.execute_command("systemctl restart docker")
            shell.execute_command(
                "for i in $(seq 1 30); do docker info >/dev/null 2>&1 "
                "&& break; sleep 1; done")

        # 3. (Re)start the real executor container against this node's
        # analytics storage directory, resolved above.
        #
        # Docker silently creates a missing bind-mount source as a
        # root-owned directory -- which is exactly the MB-73771 failure
        # mode, where CBAS (running as couchbase) can no longer write into
        # its own applications directory. Refuse rather than manufacture
        # that state and have it surface later as an unrelated error.
        apps_present, _ = shell.execute_command(
            "test -d {0} && echo present".format(applications_dir))
        if not apps_present or "present" not in apps_present[0]:
            log.error(
                "Analytics applications directory {0} does not exist on "
                "{1}; refusing to start the executor, since Docker would "
                "create it root-owned (MB-73771)".format(
                    applications_dir, server.ip))
            return False

        shell.execute_command(
            "docker rm -f {0} 2>/dev/null".format(container_name))
        shell.execute_command("rm -f {0} {0}.lock".format(socket_path))
        shell.execute_command("docker pull {0}".format(image))
        run_cmd = (
            "docker run -d --name {name} --restart unless-stopped "
            "--runtime=runsc --user {uid}:{gid} "
            "-v /var/run/udf-sockets:/var/run/udf-sockets "
            "-v {apps}:{apps}:ro "
            "{image}"
        ).format(name=container_name, uid=couchbase_uid, gid=couchbase_gid,
                  apps=applications_dir, image=image)
        shell.execute_command(run_cmd)

        # `docker run -d` returns once the container is *created*, not once
        # the executor inside it has bound its socket -- measured at ~0.5s
        # on an already-pulled image, and longer on a loaded node. Poll the
        # way the Docker restart above does, rather than checking once and
        # reporting a race as a provisioning failure.
        verify, _ = shell.execute_command(
            "for i in $(seq 1 60); do "
            "stat -c '%u:%g' {0} 2>/dev/null | grep -qx {1}:{2} && "
            "echo present && break; sleep 1; done".format(
                socket_path, couchbase_uid, couchbase_gid))
        if not verify or "present" not in verify[0]:
            # The generic message hides the usual causes (image pull
            # refused, runsc rejecting the sandbox, uid/gid mismatch), all
            # of which the container's own output names outright.
            container_log, _ = shell.execute_command(
                "docker logs --tail 20 {0} 2>&1".format(container_name))
            log.error(
                "UDF executor container started but its socket never "
                "appeared with the right ownership on {0}; last container "
                "output: {1}".format(
                    server.ip, "\n".join(container_log or ["<none>"])))
            return False
        log.info(
            "Provisioned the UDF executor container on {0}".format(
                server.ip))
        return True
    except Exception as e:
        log.error(
            "Failed to provision the UDF executor container on "
            "{0}: {1}".format(server.ip, e))
        return False
    finally:
        shell.disconnect()
