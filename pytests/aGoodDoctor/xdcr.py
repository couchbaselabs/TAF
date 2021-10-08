'''
Created on 29-Sep-2021

@author: riteshagarwal
'''
import time
from membase.api.rest_client import RestConnection


class XDCReplication:

    def __init__(self, remote_ref, src_cluster, dest_cluster, src_bkt, dest_bkt, repl_id):
        self.remote_ref = remote_ref
        self.src_cluster = src_cluster
        self.dest_cluster = dest_cluster
        self.src_bkt = src_bkt
        self.dest_bkt = dest_bkt
        self.repl_id = repl_id


class DoctorXDCR():

    def __init__(self, src_cluster, dest_cluster):
        self.src_cluster = src_cluster
        self.dest_cluster = dest_cluster
        self.src_master = self.src_cluster.master
        self.dest_master = self.dest_cluster.master
        self.src_rest = RestConnection(self.src_master)
        self.dest_rest = RestConnection(self.dest_master)
        # List of XDCReplication objects for src_cluster
        self.replications = []

    def create_remote_ref(self, name="remote"):
        '''
        Create remote cluster reference from src_cluster to remote_cluster
        :return: None
        '''
        self.src_rest.add_remote_cluster(
            self.dest_master.ip, self.dest_master.port,
            self.dest_master.rest_username,
            self.dest_master.rest_password, name)

    def create_replication(self, remote_ref, src_bkt, dest_bkt):
        '''
        Create replication in the src cluster to replicate data to dest cluster
        '''
        repl_id = self.src_rest.start_replication("continuous", src_bkt, remote_ref, "xmem", dest_bkt)
        if repl_id is not None:
            self.replications.append(XDCReplication(remote_ref, self.src_cluster, self.dest_cluster,
                                                    src_bkt, dest_bkt, repl_id))

    def set_xdcr_param(self, src_bkt, dest_bkt, param, value):
        """Set a replication setting to a value
        """
        self.src_rest.set_xdcr_param(src_bkt, dest_bkt, param, value)

    def drop_replication_by_id(self, repl_id):
        '''
        Drop replication in the src cluster
        '''
        self.src_rest.stop_replication(repl_id)
        for repl in self.replications:
            if repl_id == repl.repl_id:
                self.replications.remove(repl)

    def drop_all_replications(self):
        self.src_rest.remove_all_replications()
        self.replications = []

    def delete_remote_ref(self, name):
        self.src_rest.remove_remote_cluster(name)

    def monitor_replications(self, duration=0, print_duration=600):
        st_time = time.time()
        update_time = time.time()
        if duration == 0:
            while not self.stop_run:
                if st_time + print_duration < time.time():
                    for current_repl in self.src_rest.get_replications():
                        for repl in self.replications:
                            if current_repl['id'] == repl.repl_id:
                                print("Source bucket {0} doc count = {1}\nDest bucket {2} doc count = {3}".format(
                                    repl.src_bkt,
                                    self.bucket_util.get_bucket_current_item_count(repl.src_cluster, repl.src_bkt),
                                    repl.dest_bkt,
                                    self.bucket_util.get_bucket_current_item_count(repl.dest_cluster, repl.src_bkt)))
        else:
            while st_time + duration > time.time():
                if update_time + print_duration < time.time():
                    update_time = time.time()
