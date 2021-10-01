'''
Created on 29-Sep-2021

@author: riteshagarwal
'''
import time


class DoctorXDCR():

    def __init__(self, cluster, cluster_util, bucket_util, num_repl=10):
        self.dstn_cluster = cluster
        self.dstn_cluster_util = cluster_util
        self.dstn_bucket_util = bucket_util
        self.num_repl = num_repl
        '''
        1. Create destination cluster here
        2. Create buckets, scopes, collection in destination cluster
        '''

    def discharge_XDCR(self):
        pass

    def create_replications(self, src_cluster):
        '''
        Create replication in the src cluster to replicate data to dstn cluster
        '''
        pass

    def drop_replications(self, src_cluster):
        '''
        Drop replication in the src cluster
        '''
        pass

    def monitor_replication(self, duration=0, print_duration=600):
        st_time = time.time()
        update_time = time.time()
        if duration == 0:
            while not self.stop_run:
                if st_time + print_duration < time.time():
                    pass
        else:
            while st_time + duration > time.time():
                if update_time + print_duration < time.time():
                    update_time = time.time()
