'''
Created on 29-Sep-2021

@author: riteshagarwal
'''
import time


class DoctorXDCR():

    def __init__(self, cluster, cluster_util, bucket_util, num_repl=10):
        self.xdcr_cluster = cluster
        self.cluster_util = cluster_util
        self.bucket_util = bucket_util
        self.num_repl = num_repl

    def discharge_XDCR(self):
        pass

    def create_replications(self):
        pass

    def drop_replications(self):
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
