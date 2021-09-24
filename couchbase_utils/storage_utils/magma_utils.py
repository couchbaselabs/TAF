'''
Created on 31-May-2021

@author: riteshagarwal
'''
import os

from cb_tools.cbstats import Cbstats
from global_vars import logger
from remote.remote_util import RemoteMachineShellConnection


class MagmaUtils:
    def __init__(self):
        self.log = logger.get("test")

    def get_disk_usage(self, cluster, bucket, data_path, servers=None, units='mb'):
        """ Returns magma disk usage.

        Args:
            cluster (Cluster): The TAF cluster object.
            bucket (Bucket): The bucket who's disk usage is being fetched.
            data_path (str): A path to Couchbase's data directory.
            server (list(TestInputServer)): The servers to fetch the usage from.

        Returns:
            list: A list of disk usage by magma component (e.g. [kvstore,
            write-ahead-log, key-tree and seq-tree])
        """
        disk_usage = []
        if servers is None:
            servers = cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        kvstore = 0
        wal = 0
        keyTree = 0
        seqTree = 0

        units_to_flag = {'mb': '-cm', 'kb': '-ck', 'b': '-cb'}

        if units not in units_to_flag:
            raise ValueError("The units must be in {}".format(units_to_flag.keys()))

        flag = units_to_flag[units]

        def get_command(glob):
            path = os.path.join(data_path, bucket.name, glob)
            return "du {} {} | tail -1 | awk '{{print $1}}'".format(flag, path)

        for server in servers:
            shell = RemoteMachineShellConnection(server)
            kvstore += int(shell.execute_command(get_command("magma.*/kv*"))[0][0].split('\n')[0])
            wal += int(shell.execute_command(get_command("magma.*/wal"))[0][0].split('\n')[0])
            keyTree += int(shell.execute_command(get_command("magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command(get_command("magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])
            shell.disconnect()

        self.log.info("Disk usage stats for bucket {} is below".format(bucket.name))
        self.log.info("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}MB".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}MB".format(seqTree))
        disk_usage.extend([kvstore, wal, keyTree, seqTree])
        return disk_usage

    def get_magma_data_size(self, server, bucket_name):
        """ The size of the data the user stored in bytes. """
        remote = RemoteMachineShellConnection(server)
        result = Cbstats(remote).all_stats(bucket_name)["ep_magma_logical_data_size"]
        remote.disconnect()
        return int(result)

    def get_magma_disk_size(self, server, bucket_name):
        """ The space occupied by the data in bytes. """
        remote = RemoteMachineShellConnection(server)
        result = Cbstats(remote).all_stats(bucket_name)["ep_magma_logical_disk_size"]
        remote.disconnect()
        return int(result)

    def check_disk_usage(self, servers, buckets, fragmentation):
        """ Returns true if the disk usage exceeds the expected disk usage
        percentage. """
        for server in servers:
            for bucket in buckets:
                data_size = self.get_magma_data_size(server, bucket.name)
                disk_size = self.get_magma_disk_size(server, bucket.name)
                calc_frag = (disk_size - data_size) / float(data_size)

            self.log.info("Data size:{} Disk size:{} Calc Frag:{}".format(data_size, disk_size, calc_frag))

            if (calc_frag > fragmentation):
                return False

        return True
