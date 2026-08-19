import json
import threading
import time

from cb_tools.cb_tools_base import CbCmdBase
import logging
from cb_constants import CbServer


class CbContBk(CbCmdBase):
    # Hard wall-clock ceiling for any cbcontbk command. The largest restores
    # in the suite complete well under a minute; the ceiling only exists so
    # a dead SSH connection or hung cbcontbk process fails the test instead
    # of blocking the run indefinitely.
    CMD_TIMEOUT = 900

    def __init__(self, shell_conn, username="Administrator",
                 password="password", log=None,
                 cbcontbk_cloud_provider=None, backup_cloud_provider=None,
                 kms_provider=None, obj_staging_dir=None):
        """
        :param cbcontbk_cloud_provider: provider backing the continuous-backup
                                        location (cbcontbk's -l)
        :param backup_cloud_provider: provider backing the traditional backup
                                      archive (cbcontbk's -a)
        :param obj_staging_dir: default --obj-staging-dir for every command;
                                the staging dir is a property of the
                                node/object-store pairing rather than of an
                                individual command, so it is normally set
                                once here and overridden per call only when
                                a command needs a different one

        Every cbcontbk command touches both the archive and the continuous
        backup location, but the CLI takes only one set of object-store
        credentials -- so the two halves must live in the same object store.
        Accordingly the accepted combinations are:

          - neither set                     -- both halves on local/NFS storage
          - only one set                    -- that half is on an object store
          - both set to the same provider type

        Two different provider types raise here rather than producing a
        command that can only ever reach one of the two locations.
        """
        CbCmdBase.__init__(self, shell_conn, "cbcontbk",
                           username=username, password=password)
        self.cli_flags = ""
        self.cbcontbk_cloud_provider = cbcontbk_cloud_provider
        self.backup_cloud_provider = backup_cloud_provider
        self.kms_provider = kms_provider
        self.obj_staging_dir = obj_staging_dir
        if log:
            self.log = log
        else:
            self.log = logging.getLogger("test")

        if cbcontbk_cloud_provider is not None \
                and backup_cloud_provider is not None \
                and type(cbcontbk_cloud_provider) is not type(backup_cloud_provider):
            raise ValueError(
                "cbcontbk cannot span two object stores: continuous backup is "
                "on %s but the backup archive is on %s"
                % (type(cbcontbk_cloud_provider).__name__,
                   type(backup_cloud_provider).__name__))

    def _append_km_flags(self, cmd):
        if self.kms_provider is not None:
            cmd += " %s" % self.kms_provider.get_km_flags(self.shellConn)
        return cmd

    def _append_obj_store_flags(self, cmd, obj_staging_dir=None,
                                includes_archive_path=True):
        """
        Append the object-store flags for whichever half of the command needs
        them, plus `--obj-staging-dir`.

        `cbcontbk_cloud_provider` wins when both are set: __init__ has already
        guaranteed they are the same provider type, so its flags cover the
        archive too.

        The `backup_cloud_provider` fallback only makes sense when `cmd` also
        carries an `-a <archive_path>` for those flags to apply to (i.e.
        `restore`). `info`/`collect_logs` pass only `-l <location>`, so
        falling back to the archive's provider there attaches cloud
        credentials to a command with no cloud-schemed path at all --
        cbcontbk then rejects it with "cloud arguments provided without the
        cloud scheme prefix". `includes_archive_path=False` skips that
        fallback for those callers.

        The staging dir comes from `obj_staging_dir` if given, else the
        instance default from __init__.
        """
        staging_dir = obj_staging_dir or self.obj_staging_dir
        if staging_dir:
            cmd += f" --obj-staging-dir {staging_dir}"

        if self.cbcontbk_cloud_provider is not None:
            cmd += f" {self.cbcontbk_cloud_provider.get_cbconbk_flags(self.shellConn)}"
        elif includes_archive_path and self.backup_cloud_provider is not None:
            cmd += f" {self.backup_cloud_provider.get_cbbackupmgr_flags(self.shellConn)}"

        return cmd

    def _execute_cmd(self, cmd):
        """Run the command with a hard timeout via a daemon thread.

        Scoped to cbcontbk so the shared shell library stays untouched:
        some paramiko internals have no timeout of their own, and an
        abandoned daemon thread (unlike a ThreadPoolExecutor worker) can
        never block interpreter exit."""
        result = dict()

        def _run():
            try:
                result["value"] = CbCmdBase._execute_cmd(self, cmd)
            except Exception as e:
                result["exception"] = e

        runner = threading.Thread(target=_run, daemon=True)
        runner.start()
        runner.join(timeout=self.CMD_TIMEOUT)
        if runner.is_alive():
            raise Exception(f"cbcontbk command timed out after "
                            f"{self.CMD_TIMEOUT}s: {cmd}")
        if "exception" in result:
            raise result["exception"]
        return result["value"]

    def get_cluster_timestamp(self):
        """
        Gets the current UTC timestamp from the cluster host.
        """
        cmd = "date -u +'%Y-%m-%dT%H:%M:%SZ'"

        self.log.info(f"Executing command: {cmd}")

        output, error = self._execute_cmd(cmd)

        if error:
            self.log.error(f"Failed to get cluster timestamp: {error}")
            return None

        self.log.info(f"Command output: {output}")

        return output[0].strip()

    def _ensure_temp_dir(self, temp_dir):
        """
        Create `temp_dir` (cbcontbk's `-d` restore staging dir) if it
        doesn't already exist. Never wiped, before or after: the original
        bug this guarded against was a *literal* temp_dir shared across
        different tests (even different Jenkins builds reusing the same
        node), where stale staging state from an unrelated earlier restore
        poisoned a later one. Callers now pass a directory created once per
        test in setUp() (and logged there), so within a single test,
        multiple sequential restores sharing it is expected and safe --
        wiping between them would just destroy an earlier restore's
        evidence for no benefit.
        """
        if not temp_dir or not temp_dir.strip("/"):
            raise ValueError(f"Refusing to use unsafe temp_dir {temp_dir!r}")
        cmd = f"mkdir -p {temp_dir}"
        self.log.debug(f"Ensuring restore temp dir exists: {cmd}")
        _, error = self._execute_cmd(cmd)
        if error:
            self.log.warning(f"Failed to create temp dir {temp_dir}: {error}")

    def restore(self, archive_path, repo_name,
                location, temp_dir, cluster_host=None, threads=8, timestamp=None,
                include_data=None, map_data=None, obj_staging_dir=None):
        """
        Restores a continuous backup to a specified point in time.
        :param archive_path: Path to the traditional backup location
        :param repo_name: Name of the backup repository (e.g., "repo1")
        :param location: Location of the continuous backup
        :param temp_dir: Restore staging dir (cbcontbk's `-d`). Callers
                         should pass a directory created once at test setup
                         (and logged there) rather than a fresh path per
                         call -- see _ensure_temp_dir. Never wiped, so every
                         restore in the test (pass or fail) leaves its
                         state behind for investigation.
        :param cluster_host: Cluster address (e.g., "localhost:8091")
        :param threads: Number of threads to use for the restore (default: 8)
        :param timestamp: Timestamp in UTC for the point-in-time recovery.
                          If not provided, defaults to "everything" (restores
                          up to the latest data cbcontbk has backed up).
                          cbcontbk rejects a restore whose target timestamp
                          is not contained within the backup, so callers
                          must not pass the current cluster time as a stand-in
                          for "latest" -- use "everything" for that instead.
        :param include_data: Specific collection to include
        :param map_data: Mapping for the data restore
        :param obj_staging_dir: Object staging dir, appended as
                                --obj-staging-dir
        """
        if cluster_host is None:
            cluster_host = f"http://{self.shellConn.server.ip}:8091"

        if timestamp is None:
            timestamp = "everything"

        self._ensure_temp_dir(temp_dir)

        cmd = (f"{self.cbstatCmd} restore -a {archive_path} -r {repo_name} "
               f"-c {cluster_host} -u {self.username} -p {self.password} "
               f"-t {threads} -l {location} -d {temp_dir} -T {timestamp}")

        if include_data:
            cmd += f" --include-data {include_data}"
        if map_data:
            cmd += f" --map-data {map_data}"

        cmd += self.cli_flags

        cmd = self._append_obj_store_flags(cmd, obj_staging_dir)

        cmd = self._append_km_flags(cmd)

        self.log.info(f"Executing command: {cmd}")

        output, error = self._execute_cmd(cmd)

        self.log.info(f"Command output: {output}")

        if not output or error:
            self.log.error(f"Continuous backup restore failed with: {error}")

        return output, error

    def info(self, location):
        """
        Returns cbcontbk's own authoritative view of what has been captured
        at `location`, as {bucket_name: bucket_info_dict}, where
        bucket_info_dict["range"]["end"] (ISO-8601) is how far the
        continuous log actually extends for that bucket.

        This is real bookkeeping from cbcontbk itself, not an inference
        from the log store's own object timestamps -- the latter can
        plateau for minutes while cbcontbk is still catching up (confirmed
        against a real CI failure: object uploads went quiet for 45s+ while
        cbcontbk's own `info` showed the log was still ~8 minutes from
        actually catching up).

        Returns None if the command fails or the output isn't valid JSON.
        """
        cmd = f"{self.cbstatCmd} info -l {location} --json"
        cmd += self.cli_flags
        cmd = self._append_obj_store_flags(cmd, includes_archive_path=False)

        self.log.info(f"Executing command: {cmd}")

        output, error = self._execute_cmd(cmd)

        self.log.info(f"Command output: {output}")

        if error or not output:
            self.log.error(f"cbcontbk info failed: {error}")
            return None

        try:
            parsed = json.loads("".join(output))
        except ValueError as e:
            self.log.error(
                f"cbcontbk info returned unparseable JSON: {e}. "
                f"Output: {output}")
            return None

        return {bucket["name"]: bucket
               for bucket in parsed.get("buckets", [])}

    def collect_logs(self, location, temp_dir, obj_staging_dir=None):
        """
        Collects logs for a continuous backup.
        :param location: Location of the continuous backup
        :param temp_dir: Temporary directory for log collection
        :param obj_staging_dir: Object staging dir, appended as
                                --obj-staging-dir
        """
        cmd = (f"{self.cbstatCmd} collect-logs -l {location} "
               f"-d {temp_dir}")

        cmd += self.cli_flags

        cmd = self._append_obj_store_flags(cmd, obj_staging_dir,
                                           includes_archive_path=False)

        cmd = self._append_km_flags(cmd)

        self.log.info(f"Executing command: {cmd}")

        output, error = self._execute_cmd(cmd)

        self.log.info(f"Command output: {output}")

        if not output or error:
            self.log.error(f"Command failed with error: {error}")

        return output, error
