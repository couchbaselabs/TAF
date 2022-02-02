'''
Created on 29-Sep-2021

@author: riteshagarwal
'''
import time
from remote.remote_util import RemoteMachineShellConnection
import os
from global_vars import logger


class DoctorBKRS():

    def __init__(self, cluster):
        self.log = logger.get("infra")
        self.cluster = cluster
        shell = RemoteMachineShellConnection(self.cluster.master)
        bin_path = shell.return_bin_path_based_on_os(shell.return_os_type())
        shell.disconnect()
        self.cbbackupmgr = os.path.join(bin_path, "cbbackupmgr")

    def configure_backup(self, archive, repo, exclude, include):

        shell = RemoteMachineShellConnection(self.cluster.backup_nodes[0])
        shell.log.info('Delete previous backups')
        command = 'rm -rf %s' % archive
        o, r = shell.execute_command(command)
        shell.log_command_output(o, r)

        shell.log.info('Configure backup')
        configure_bkup_cmd = '{0} config -a {1} -r {2}'.format(
            self.cbbackupmgr, archive, repo)

        if exclude:
            configure_bkup_cmd += ' --exclude-data ' + ",".join(exclude)
        if include:
            configure_bkup_cmd += ' --include-data ' + ",".join(include)
        print(configure_bkup_cmd)
        o, r = shell.execute_command(configure_bkup_cmd)
        shell.log_command_output(o, r)
        shell.disconnect()

    def trigger_backup(self, archive='/data/backups', repo='magma',
                       username="Administrator", password="password"):

        shell = RemoteMachineShellConnection(self.cluster.backup_nodes[0])
        bkup_cmd = '{0} backup -a {1} -r {2} --cluster couchbase://{3} --username {4} --password {5} &'.format(
            self.cbbackupmgr, archive, repo, self.cluster.master.ip, username, password)
        print(bkup_cmd)
        o, r = shell.execute_command(bkup_cmd)
        print o
        print r
        shell.disconnect()
        return o

    def trigger_restore(self, archive='/data/backups', repo='example',
                        username="Administrator", password="password"):

        shell = RemoteMachineShellConnection(self.cluster.backup_nodes[0])
        shell.log.info('Restore backup using cbbackupmgr')
        restore_cmd = '{0} restore -a {1} -r {2} --cluster couchbase://{3} --username {4} --password {5} &'.format(
            self.cbbackupmgr, archive, repo, self.cluster.master.ip, username, password)
        print(restore_cmd)
        o, r = shell.execute_command(restore_cmd)
        shell.log_command_output(o, r)
        shell.disconnect()
        return o

    def monitor_restore(self, bucket_util, items, timeout=43200):
        end_time = time.time() + timeout
        while time.time() < end_time:
            curr_items = bucket_util.get_bucket_current_item_count(self.cluster, self.cluster.buckets[0])
            self.log.info("Current/Expected items during restore: %s == %s" % (curr_items, items) )
            if items == curr_items:
                return True
        self.log.info("cbbackupmgr restore did not finish in %s seconds: Actual:%s, Expected:%s" % (timeout, curr_items, items) )
        return False