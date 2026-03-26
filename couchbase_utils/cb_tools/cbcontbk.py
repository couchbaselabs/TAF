from cb_tools.cb_tools_base import CbCmdBase
import logging
from cb_constants import CbServer


class CbContBk(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password", log=None):
        CbCmdBase.__init__(self, shell_conn, "cbcontbk",
                           username=username, password=password)
        self.cli_flags = ""
        if log:
            self.log = log
        else:
            self.log = logging.getLogger("test")

    def get_cluster_timestamp(self):
        """
        Gets the current UTC timestamp from the cluster host.
        """
        cmd = "date -u +'%Y-%m-%dT%H:%M:%SZ'"
        
        self.log.debug(f"Executing command: {cmd}")
            
        output, error = self._execute_cmd(cmd)
        
        if error:
            self.log.error(f"Failed to get cluster timestamp: {error}")
            return None
            
        self.log.debug(f"Command output: {output}")
            
        return output[0].strip()

    def restore(self, archive_path, repo_name,
                location, temp_dir, cluster_host=None, threads=8, timestamp=None,
                include_data=None, map_data=None):
        """
        Restores a continuous backup to a specified point in time.
        :param archive_path: Path to the traditional backup location
        :param repo_name: Name of the backup repository (e.g., "repo1")
        :param location: Location of the continuous backup
        :param temp_dir: Temporary directory for restore operations
        :param cluster_host: Cluster address (e.g., "localhost:8091")
        :param threads: Number of threads to use for the restore (default: 8)
        :param timestamp: Timestamp in UTC for the point-in-time recovery.
                          If not provided, defaults to the current UTC time from the cluster.
        :param include_data: Specific collection to include
        :param map_data: Mapping for the data restore
        """
        if cluster_host is None:
            cluster_host = f"http://{self.shellConn.server.ip}:8091"

        if timestamp is None:
            timestamp = self.get_cluster_timestamp()
            if not timestamp:
                raise Exception("Could not retrieve cluster timestamp for restore.")

        cmd = (f"{self.cbstatCmd} restore -a {archive_path} -r {repo_name} "
               f"-c {cluster_host} -u {self.username} -p {self.password} "
               f"-t {threads} -l {location} -d {temp_dir} -T {timestamp}")

        if include_data:
            cmd += f" --include-data {include_data}"
        if map_data:
            cmd += f" --map-data {map_data}"

        cmd += self.cli_flags
        
        self.log.debug(f"Executing command: {cmd}")
            
        output, error = self._execute_cmd(cmd)
        
        self.log.debug(f"Command output: {output}")
            
        if not output or error:
            self.log.error(f"Continuous backup restore failed with: {error}")
            
        return output, error

    def collect_logs(self, location, temp_dir):
        """
        Collects logs for a continuous backup.
        :param location: Location of the continuous backup
        :param temp_dir: Temporary directory for log collection
        """
        cmd = (f"{self.cbstatCmd} collect-logs -l {location} "
               f"-d {temp_dir}")

        cmd += self.cli_flags
        
        self.log.debug(f"Executing command: {cmd}")
            
        output, error = self._execute_cmd(cmd)
        
        self.log.debug(f"Command output: {output}")
            
        if not output or error:
            self.log.error(f"Command failed with error: {error}")

        return output, error
