"""
Minimal cbbackupmgr CLI wrapper for master_jython.

Ported from TAF master's couchbase_utils/cb_tools/cbbackupmgr.py, trimmed
to only the operations exercised by the CE EE-feature-restriction tests
(pytests/ns_server/ce_ee_feature_restriction_tests.py::
test_backup_ee_features_blocked_cli): config (repo create), backup,
merge and examine. Follows the same CbCmdBase convention already used by
cb_tools/cb_cli.py on this branch -- shell_conn.execute_command() runs
the couchbase-cli/cbbackupmgr binary remotely and returns (output, error)
line lists, so no result here is auto-JSON-decoded or exception-raising;
callers inspect the returned lists directly (matching master's behavior).
"""

from cb_tools.cb_tools_base import CbCmdBase
from cb_constants import CbServer


class CbBackupMgr(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password", no_ssl_verify=None):
        CbCmdBase.__init__(self, shell_conn, "cbbackupmgr",
                           username=username, password=password)
        if no_ssl_verify is None:
            no_ssl_verify = CbServer.use_https
        self.cli_flags = ""
        if no_ssl_verify:
            self.cli_flags += " --no-ssl-verify"

    def create_repo(self, archive_dir, repo_name, exclude=None, include=None,
                    worm_period=None, default_retention=None):
        """
        Execute cbbackupmgr config command to create a repository.

        :param archive_dir: Backup archive directory.
        :param repo_name: Backup repository name.
        :param include: List of scope.collection strings to restrict the
               repo's backups to (EE-only per cbbackupmgr-config docs).
        """
        cmd = "%s config --archive %s --repo %s" % (
            self.cbstatCmd, archive_dir, repo_name)

        if exclude:
            cmd += " --exclude-data " + ",".join(exclude)
        if include:
            cmd += " --include-data " + ",".join(include)
        if worm_period is not None:
            cmd += " --worm %s" % worm_period
        if default_retention is not None:
            cmd += " --default-retention %s" % default_retention

        return self._execute_cmd(cmd)

    def backup(self, archive_dir, repo_name, cluster_host=None,
              consistency_check=None):
        """
        Execute cbbackupmgr backup command.

        :param archive_dir: Backup archive directory. Prefix with
               "s3://<bucket_name>/" to back up directly to S3
               (EE-only).
        :param consistency_check: Consistency check window in seconds
               (EE-only).
        """
        if cluster_host is None:
            if CbServer.use_https:
                cluster_host = "https://%s:%s" % (self.shellConn.ip, self.port)
            else:
                cluster_host = "http://%s:%s" % (self.shellConn.ip, self.port)

        cmd = "%s backup --archive %s --repo %s --cluster %s -u %s -p %s" % (
            self.cbstatCmd, archive_dir, repo_name, cluster_host,
            self.username, self.password)

        if consistency_check:
            cmd += " --consistency-check %d" % consistency_check

        cmd += self.cli_flags
        return self._execute_cmd(cmd)

    def merge(self, archive_dir, repo_name, start=None, end=None):
        """
        Execute cbbackupmgr merge command to merge a range of backups
        (EE-only).
        """
        cmd = "%s merge --archive %s --repo %s" % (
            self.cbstatCmd, archive_dir, repo_name)
        if start is not None and end is not None:
            cmd += " --start %s --end %s" % (start, end)
        return self._execute_cmd(cmd)

    def examine(self, archive_dir, repo_name, key, collection_string=None):
        """
        Execute cbbackupmgr examine command to inspect a document by key
        (EE-only).

        :param key: Document key to examine.
        :param collection_string: Optional scope.collection.key path.
        """
        cmd = "%s examine --archive %s --repo %s -k %s" % (
            self.cbstatCmd, archive_dir, repo_name, key)
        if collection_string:
            cmd += " --collection-string %s" % collection_string
        cmd += self.cli_flags
        return self._execute_cmd(cmd)
