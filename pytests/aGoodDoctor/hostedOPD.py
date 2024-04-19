'''
Created on 30-Aug-2021

@author: riteshagarwal
'''
import time

from opd import OPD


class hostedOPD(OPD):
    def __init__(self):
        pass

    def threads_calculation(self):
        self.process_concurrency = self.input.param("pc", self.process_concurrency)
        self.doc_loading_tm = TaskManager(self.process_concurrency*self.num_tenants*self.num_clusters*self.num_buckets)

    def print_cluster_cpu_ram(self, cluster, step=300):
        while not self.stop_run:
            try:
                self.cluster_util.print_cluster_stats(cluster)
            except:
                pass
            time.sleep(step)
