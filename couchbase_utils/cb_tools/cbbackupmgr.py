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

    """
    Method to backup a Couchbase cluster using cbbackupmgr backup command
    
    :param archive_dir str: The location of the backup archive directory. 
                            When backing up directly to S3, prefix with s3://${BUCKET_NAME}/
    :param repo_name str: The name of the backup repository to backup data into
    :param cluster_host str: The hostname of one of the nodes in the cluster to back up
    :param resume bool: If True, resume a previous backup that did not complete successfully
    :param purge bool: If True, remove partial backup and restart from previous successful backup
    :param threads int: Number of concurrent clients to use when taking a backup
    :param no_progress_bar bool: If True, suppress progress bar output
    :param full_backup bool: If True, perform a full backup instead of incremental
    :param value_compression str: Compression policy for backed up values. 
                                  Options: "unchanged", "uncompressed", "compressed" (default: "compressed")
    :param skip_last_compaction bool: If True, skip the last compaction checkpoint
    :param consistency_check int: Consistency check window in seconds (Enterprise Edition feature)
    """
    def backup(self, archive_dir, repo_name, cluster_host=None,
               resume=False, purge=False, threads=None,
               no_progress_bar=False, full_backup=False,
               value_compression=None, skip_last_compaction=False,
               consistency_check=None):
        """
        Execute cbbackupmgr backup command
        """
        if cluster_host is None:
            if CbServer.use_https:
                cluster_host = f"https://{self.shellConn.server.ip}:{self.port}"
            else:
                cluster_host = f"http://{self.shellConn.server.ip}:{self.port}"

        cmd = "%s backup --archive %s --repo %s --cluster %s -u %s -p %s" % (
            self.cbstatCmd, archive_dir, repo_name, cluster_host,
            self.username, self.password)

        if resume:
            cmd += " --resume"

        if purge:
            cmd += " --purge"

        if threads:
            cmd += " --threads %d" % threads

        if no_progress_bar:
            cmd += " --no-progress-bar"

        if full_backup:
            cmd += " --full-backup"

        if value_compression:
            cmd += " --value-compression %s" % value_compression

        if skip_last_compaction:
            cmd += " --skip-last-compaction"

        if consistency_check:
            cmd += " --consistency-check %d" % consistency_check

        cmd += self.cli_flags

        output, error = self._execute_cmd(cmd)
        return output, error

    """
    Method to create a backup repository using cbbackupmgr config command
    
    :param archive_dir str: The location of the backup archive directory
    :param repo_name str: The name of the backup repository to create
    """
    def create_repo(self, archive_dir, repo_name):
        """
        Execute cbbackupmgr config command to create a repository
        """
        cmd = "%s config --archive %s --repo %s" % (
            self.cbstatCmd, archive_dir, repo_name)

        output, error = self._execute_cmd(cmd)
        return output, error

    """
    Method to list backups in a repository using cbbackupmgr list command
    
    :param archive_dir str: The location of the backup archive directory
    :param repo_name str: The name of the backup repository
    """
    def list_backups(self, archive_dir, repo_name):
        """
        Execute cbbackupmgr list command to list backups
        """
        cmd = "%s list --archive %s --repo %s" % (
            self.cbstatCmd, archive_dir, repo_name)

        cmd += self.cli_flags

        output, error = self._execute_cmd(cmd)
        return output, error

    """
    Method to restore a backup to a Couchbase cluster using cbbackupmgr restore command
    
    :param archive_dir str: The location of the backup archive directory
    :param repo_name str: The name of the backup repository to restore from
    :param cluster_host str: The hostname of one of the nodes in the cluster to restore to
    :param backup_id str: Optional backup ID to restore (if not specified, restores latest)
    :param no_progress_bar bool: If True, suppress progress bar output
    :param threads int: Number of concurrent clients to use when restoring
    :param auto_create_buckets bool: If True, automatically create buckets during restore
    :param map_data bool: If True, map data during restore
    :param map_indexes bool: If True, map indexes during restore
    :param map_views bool: If True, map views during restore
    :param map_gsi_indexes bool: If True, map GSI indexes during restore
    :param map_ft_indexes bool: If True, map full-text indexes during restore
    :param map_analytics bool: If True, map analytics during restore
    :param map_eventing bool: If True, map eventing during restore
    :param filter_keys str: Optional filter for keys to restore
    :param filter_values str: Optional filter for values to restore
    """
    def restore(self, archive_dir, repo_name, cluster_host=None,
                backup_id=None, no_progress_bar=False, threads=None,
                auto_create_buckets=False,
                map_data=True, map_indexes=True, map_views=True,
                map_gsi_indexes=True, map_ft_indexes=True,
                map_analytics=True, map_eventing=True,
                filter_keys=None, filter_values=None):
        """
        Execute cbbackupmgr restore command
        """
        if cluster_host is None:
            if CbServer.use_https:
                cluster_host = f"https://{self.shellConn.server.ip}:{self.port}"
            else:
                cluster_host = f"http://{self.shellConn.server.ip}:{self.port}"

        cmd = "%s restore --archive %s --repo %s --cluster %s -u %s -p %s" % (
            self.cbstatCmd, archive_dir, repo_name, cluster_host,
            self.username, self.password)

        if backup_id:
            cmd += " --backup %s" % backup_id

        if no_progress_bar:
            cmd += " --no-progress-bar"

        if threads:
            cmd += " --threads %d" % threads

        if auto_create_buckets:
            cmd += " --auto-create-buckets"

        if not map_data:
            cmd += " --disable-data"
        if not map_indexes:
            cmd += " --disable-indexes"
        if not map_views:
            cmd += " --disable-views"
        if not map_gsi_indexes:
            cmd += " --disable-gsi-indexes"
        if not map_ft_indexes:
            cmd += " --disable-ft-indexes"
        if not map_analytics:
            cmd += " --disable-analytics"
        if not map_eventing:
            cmd += " --disable-eventing"

        if filter_keys:
            cmd += " --filter-keys %s" % filter_keys
        if filter_values:
            cmd += " --filter-values %s" % filter_values

        cmd += self.cli_flags

        output, error = self._execute_cmd(cmd)
        return output, error

    """
    Method to remove a backup repository using cbbackupmgr remove command
    
    :param archive_dir str: The location of the backup archive directory
    :param repo_name str: The name of the backup repository to remove
    :param backup_range str: Optional backup range to remove (e.g., "1-5" or "1,3,5")
    """
    def remove(self, archive_dir, repo_name, backup_range=None):
        """
        Execute cbbackupmgr remove command to remove a repository or specific backups
        """
        cmd = "%s remove --archive %s --repo %s" % (
            self.cbstatCmd, archive_dir, repo_name)
        
        if backup_range:
            cmd += " --backups %s" % backup_range
        
        output, error = self._execute_cmd(cmd)
        return output, error

