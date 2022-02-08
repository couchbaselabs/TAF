'''
Created on 15-Apr-2021

@author: riteshagarwal
'''
from basetestcase import BaseTestCase
from remote.remote_util import RemoteMachineShellConnection
from Cb_constants.CBServer import CbServer
from membase.api.rest_client import RestConnection
from aGoodDoctor.cbas import DoctorCBAS
from aGoodDoctor.n1ql import DoctorN1QL
from aGoodDoctor.xdcr import DoctorXDCR
from fts_utils.fts_ready_functions import FTSUtils
from aGoodDoctor.opd import OPD
from BucketLib.BucketOperations import BucketHelper
from cluster_utils.cluster_ready_functions import CBCluster
import threading
import random
from aGoodDoctor.bkrs import DoctorBKRS
import os


class Murphy(BaseTestCase, OPD):

    def init_doc_params(self):
        self.create_perc = self.input.param("create_perc", 100)
        self.update_perc = self.input.param("update_perc", 20)
        self.delete_perc = self.input.param("delete_perc", 20)
        self.expiry_perc = self.input.param("expiry_perc", 20)
        self.read_perc = self.input.param("read_perc", 20)
        self.start = 0
        self.end = 0
        self.initial_items = self.start
        self.final_items = self.end
        self.create_end = 0
        self.create_start = 0
        self.update_end = 0
        self.update_start = 0
        self.delete_end = 0
        self.delete_start = 0
        self.expire_end = 0
        self.expire_start = 0

    def setUp(self):
        BaseTestCase.setUp(self)
        self.init_doc_params()

        self.num_collections = self.input.param("num_collections", 1)
        self.xdcr_collections = self.input.param("xdcr_collections", self.num_collections)
        self.num_collections_bkrs = self.input.param("num_collections_bkrs", self.num_collections)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.xdcr_scopes = self.input.param("xdcr_scopes", self.num_scopes)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.kv_nodes = self.nodes_init
        self.cbas_nodes = self.input.param("cbas_nodes", 0)
        self.fts_nodes = self.input.param("fts_nodes", 0)
        self.index_nodes = self.input.param("index_nodes", 0)
        self.backup_nodes = self.input.param("backup_nodes", 0)
        self.xdcr_remote_nodes = self.input.param("xdcr_remote_nodes", 0)
        self.num_indexes = self.input.param("num_indexes", 0)
        self.mutation_perc = 100
        self.doc_ops = self.input.param("doc_ops", "create")
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(':')

        self.threads_calculation()
        self.rest = RestConnection(self.servers[0])
        self.op_type = self.input.param("op_type", "create")
        self.dgm = self.input.param("dgm", None)
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.num_buckets = self.input.param("num_buckets", 1)
        self.mutate = 0
        self.iterations = self.input.param("iterations", 10)
        self.step_iterations = self.input.param("step_iterations", 1)
        self.rollback = self.input.param("rollback", True)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.end_step = self.input.param("end_step", None)
        self.key_prefix = "Users"
        self.crashes = self.input.param("crashes", 20)
        self.check_dump_thread = True
        self.skip_read_on_error = False
        self.suppress_error_table = False
        self.track_failures = self.input.param("track_failures", True)
        self.loader_dict = None
        self.parallel_reads = self.input.param("parallel_reads", False)
        self._data_validation = self.input.param("data_validation", True)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.key_type = self.input.param("key_type", "SimpleKey")
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.cursor_dropping_checkpoint = self.input.param(
            "cursor_dropping_checkpoint", None)
        self.assert_crashes_on_load = self.input.param("assert_crashes_on_load",
                                                       True)
        #######################################################################
        self.PrintStep("Step 1: Create a %s node cluster" % self.nodes_init)
        if self.nodes_init > 1:
            nodes_init = self.cluster.servers[1:self.nodes_init]
            self.task.rebalance([self.cluster.master], nodes_init, [])
            self.cluster.nodes_in_cluster.extend(
                [self.cluster.master] + nodes_init)
        else:
            self.cluster.nodes_in_cluster.extend([self.cluster.master])
        self.available_servers = self.cluster.servers[len(self.cluster.nodes_in_cluster):]
        self.cluster.kv_nodes = self.cluster.nodes_in_cluster[:]
        self.cluster.kv_nodes.remove(self.cluster.master)
        self.cluster_util.set_metadata_purge_interval(self.cluster.master,
                                                      interval=self.bucket_purge_interval)
        if self.xdcr_remote_nodes > 0:
            self.assertTrue(self.xdcr_remote_nodes <= len(self.available_servers),
                            "Only {0} nodes available, cannot create XDCR remote with {1} nodes".format(
                                len(self.available_servers), self.xdcr_remote_nodes))
            remote_nodes = self.available_servers[0:self.xdcr_remote_nodes]
            self.available_servers = self.available_servers[self.xdcr_remote_nodes:]
            self.PrintStep("Step 1*: Create a %s node XDCR remote cluster" % self.xdcr_remote_nodes)
            self.xdcr_remote_cluster = CBCluster(name="remote", servers=remote_nodes,
                                                 vbuckets=self.vbuckets)
            self.task.rebalance([self.xdcr_remote_cluster.master], remote_nodes[1:], [])
            self.xdcr_remote_cluster.nodes_in_cluster.extend(remote_nodes)
        #######################################################################
        self.PrintStep("Step 2: Create required buckets and collections.")
        self.create_required_buckets(self.cluster)
        self.create_required_collections(self.cluster, self.num_scopes, self.num_collections)
        if self.xdcr_remote_nodes > 0:
            self.PrintStep("Step 2*: Create required buckets and collections on XDCR remote.")
            self.create_required_buckets(cluster=self.xdcr_remote_cluster)
            # Increase number of collections to increase number of replicated docs
            self.create_required_collections(self.xdcr_remote_cluster, self.xdcr_scopes, self.xdcr_collections)
            self.drXDCR = DoctorXDCR(self.cluster, self.xdcr_remote_cluster)

        self.rest = RestConnection(self.cluster.master)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        self.max_commit_points = self.input.param("max_commit_points", None)
        props = "magma"

        if self.max_commit_points is not None:
            props += ";magma_max_checkpoints={}".format(self.max_commit_points)
            self.log.debug("props== {}".format(props))
            self.bucket_util.update_bucket_props(
                    "backend", props,
                    self.cluster, self.cluster.buckets)

        server = self.rest.get_nodes_self()
        if self.cbas_nodes>0:
            self.rest.set_service_mem_quota({CbServer.Settings.CBAS_MEM_QUOTA:
                                             int(server.mcdMemoryReserved - 100
                                                 )})
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster.nodes_in_cluster,
                                self.servers[nodes:nodes+self.cbas_nodes], [],
                                services=["cbas"]*self.cbas_nodes)
            self.cluster.cbas_nodes = self.servers[nodes:nodes+self.cbas_nodes]
            self.cluster.nodes_in_cluster.extend(self.cluster.cbas_nodes)
            DoctorCBAS(self.cluster, self.bucket_util,
                       self.num_indexes)
            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in self.cluster.cbas_nodes]

        if self.backup_nodes:
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster.nodes_in_cluster,
                                self.servers[nodes:nodes+self.backup_nodes], [],
                                services=["backup"]*self.backup_nodes)
            self.cluster.backup_nodes = self.servers[nodes:nodes+self.backup_nodes]
            self.cluster.nodes_in_cluster.extend(self.cluster.backup_nodes)
            self.drBackup = DoctorBKRS(self.cluster)

        if self.index_nodes>0:
            self.rest.set_service_mem_quota({CbServer.Settings.INDEX_MEM_QUOTA:
                                             int(server.mcdMemoryReserved - 100
                                                 )})
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster.nodes_in_cluster,
                                self.servers[nodes:nodes+self.index_nodes], [],
                                services=["index,n1ql"]*self.index_nodes)
            self.cluster.index_nodes = self.servers[nodes:nodes+self.index_nodes]
            self.cluster.query_nodes = self.servers[nodes:nodes+self.index_nodes]
            self.cluster.nodes_in_cluster.extend(self.cluster.index_nodes)
            self.drIndex = DoctorN1QL(self.cluster, self.bucket_util,
                                             self.num_indexes)
            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in self.cluster.index_nodes]

        if self.fts_nodes>0:
            self.rest.set_service_mem_quota({CbServer.Settings.FTS_MEM_QUOTA:
                                             int(server.mcdMemoryReserved - 100
                                                 )})
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster.nodes_in_cluster,
                                self.servers[nodes:nodes+self.fts_nodes], [],
                                services=["fts"]*self.fts_nodes)
            self.cluster.fts_nodes = self.servers[nodes:nodes+self.fts_nodes]
            self.cluster.nodes_in_cluster.extend(self.cluster.fts_nodes)
            self.fts_util = FTSUtils(self.cluster, self.cluster_util, self.task)
            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in self.cluster.fts_nodes]

        print self.available_servers
        self.writer_threads = self.input.param("writer_threads", "disk_io_optimized")
        self.reader_threads = self.input.param("reader_threads", "disk_io_optimized")
        self.storage_threads = self.input.param("storage_threads", 40)
        self.set_num_writer_and_reader_threads(
                num_writer_threads=self.writer_threads,
                num_reader_threads=self.reader_threads,
                num_storage_threads=self.storage_threads)

    def tearDown(self):
        self.check_dump_thread = False
        self.stop_crash = True
        BaseTestCase.tearDown(self)

    def SteadyStateVolume(self):
        self.loop = 1
        self.create_perc = 100

        self.PrintStep("Step 1: Create %s items: %s" % (self.num_items, self.key_type))
        self.generate_docs(doc_ops=["create"],
                           create_start=0,
                           create_end=self.num_items)
        self.perform_load(validate_data=False)

        self.PrintStep("Step 2: Create %s items: %s" % (self.num_items, self.key_type))
        self.generate_docs(doc_ops=["create"],
                           create_start=self.num_items,
                           create_end=self.num_items*2)
        self.perform_load(validate_data=False)

        if self.index_nodes:
            self.drIndex.create_indexes()
            self.drIndex.build_indexes()
            self.drIndex.wait_for_indexes_online(self.log, self.drIndex.indexes)
            self.drIndex.start_query_load()

        if self.xdcr_remote_nodes > 0:
            self.drXDCR.create_remote_ref("magma_xdcr")
            for bucket in self.cluster.buckets:
                self.drXDCR.create_replication("magma_xdcr", bucket.name, bucket.name)

        self.stop_stats = False
        stat_th = threading.Thread(target=self.dump_magma_stats,
                                   kwargs=dict(server=self.cluster.master,
                                               bucket=self.cluster.buckets[0],
                                               shard=0,
                                               kvstore=0))
        stat_th.start()

        while self.loop <= self.iterations:
            #######################################################################
            '''
            creates: 0 - 10M
            deletes: 0 - 10M
            Final Docs = 0
            '''
            self.generate_docs()
            self.perform_load(validate_data=True)
            self.loop += 1
        self.stop_stats = True
        stat_th.join()

        # Starting the backup here.
        if self.backup_nodes > 0:
            self.restore_timeout = self.input.param("restore_timeout", 12*60*60)
            archive = os.path.join(self.cluster.backup_nodes[0].data_path, "bkrs")
            repo = "magma"
            self.drBackup.configure_backup(archive, repo, [], [])
            self.drBackup.trigger_backup(archive, repo)
            items = self.bucket_util.get_bucket_current_item_count(self.cluster,
                                                                   self.cluster.buckets[0])
            self.bucket_util.flush_all_buckets(self.cluster)
            self.drBackup.trigger_restore(archive, repo)
            result = self.drBackup.monitor_restore(self.bucket_util, items, timeout=self.restore_timeout)
            self.assertTrue(result, "Restore failed")

    def test_rebalance(self):
        self.loop = 1
        self.skip_read_on_error = True
        self.suppress_error_table = True
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        '''
        Create sequential: 0 - 10M
        Final Docs = 10M (0-10M, 10M seq items)
        '''
        self.create_perc = 100
        self.PrintStep("Step 2: Create %s items sequentially" % self.num_items)
        self.generate_docs(doc_ops=["create"],
                           create_start=0,
                           create_end=self.num_items)
        self.perform_load(validate_data=False)

        self.PrintStep("Step 2.1: Update %s RandonKey keys to create 50 percent fragmentation" % str(self.num_items))
        self.update_perc = 100
        self.generate_docs(doc_ops=["update"],
                           update_start=self.start,
                           update_end=self.end)
        self.perform_load(validate_data=False)

        self.PrintStep("Step 3: Create %s items sequentially" % self.num_items)
        self.generate_docs(doc_ops=["create"],
                           create_start=self.num_items,
                           create_end=self.num_items*2)
        self.perform_load(validate_data=False)

        self.PrintStep("Step 3.1: Update %s RandonKey keys to create 50 percent fragmentation" % str(self.num_items))
        self.update_perc = 100
        self.generate_docs(doc_ops=["update"],
                           update_start=self.start,
                           update_end=self.end)
        self.perform_load(validate_data=False)
#         self.stop_stats = False
#         stat_th.join()

        if self.index_nodes:
            self.drIndex.create_indexes()
            self.drIndex.build_indexes()
            self.drIndex.wait_for_indexes_online(self.log, self.drIndex.indexes)
            self.drIndex.start_query_load()

        if self.xdcr_remote_nodes > 0:
            self.drXDCR.create_remote_ref("magma_xdcr")
            for bucket in self.cluster.buckets:
                self.drXDCR.create_replication("magma_xdcr", bucket.name, bucket.name)

        self.rebl_nodes = self.input.param("rebl_nodes", 0)
        self.max_rebl_nodes = self.input.param("max_rebl_nodes", 1)
        self.doc_ops = self.input.param("doc_ops", "expiry").split(":")
        perc = 100/len(self.doc_ops)
        self.expiry_perc = perc
        self.create_perc = perc
        self.update_perc = perc
        self.delete_perc = perc
        self.read_perc = perc

        self.rebl_services = self.input.param("rebl_services", "kv").split("-")
        self.sleep(30)
        self.mutation_perc = self.input.param("mutation_perc", 20)
        while self.loop <= self.iterations:
            self.ops_rate = self.input.param("rebl_ops_rate", self.ops_rate)
            self.generate_docs()
            tasks = self.perform_load(wait_for_load=False)
            self.rebl_nodes += 1
            if self.rebl_nodes > self.max_rebl_nodes:
                self.rebl_nodes = 1
            for service in self.rebl_services:
                ###################################################################
                self.PrintStep("Step 4.{}: Rebalance IN with Loading of docs".
                               format(self.loop))
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes,
                                                nodes_out=0,
                                                services=[service]*self.rebl_nodes,
                                                retry_get_process_num=3000)
                self.sleep(60, "Sleep for 60s for rebalance to start")
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats()
                self.sleep(10, "Sleep for 60s after rebalance")

                self.PrintStep("Step 5.{}: Rebalance OUT with Loading of docs".
                               format(self.loop))
                rebalance_task = self.rebalance(nodes_in=0,
                                                nodes_out=self.rebl_nodes,
                                                services=[service],
                                                retry_get_process_num=3000)
                self.sleep(60, "Sleep for 60s for rebalance to start")
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats()
                self.sleep(10, "Sleep for 60s after rebalance")

                self.PrintStep("Step 6.{}: Rebalance SWAP with Loading of docs".
                               format(self.loop))
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes,
                                                nodes_out=self.rebl_nodes,
                                                services=[service]*self.rebl_nodes,
                                                retry_get_process_num=3000)
                self.sleep(60, "Sleep for 60s for rebalance to start")
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats()
                self.sleep(10, "Sleep for 60s after rebalance")

                self.PrintStep("Step 7.{}: Rebalance IN/OUT with Loading of docs".
                               format(self.loop))
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes+1,
                                                nodes_out=self.rebl_nodes,
                                                services=[service]*(self.rebl_nodes+1),
                                                retry_get_process_num=3000)
                self.sleep(60, "Sleep for 60s for rebalance to start")
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats()
                self.sleep(10, "Sleep for 60s after rebalance")

                self.PrintStep("Step 8.{}: Rebalance OUT/IN with Loading of docs".
                               format(self.loop))
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes,
                                                nodes_out=self.rebl_nodes+1,
                                                services=[service]*self.rebl_nodes,
                                                retry_get_process_num=3000)
                self.sleep(60, "Sleep for 60s for rebalance to start")
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats()
                self.sleep(10, "Sleep for 60s after rebalance")

                self.loop += 1
            self.wait_for_doc_load_completion(tasks)
            if self.track_failures:
                self.data_validation()

    def SystemTestMagma(self):
        #######################################################################
        self.loop = 1
        self.PrintStep("Step 1: Create %s items sequentially" % self.num_items)
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()
        self.key_type = "RandomKey"
        self.stop_rebalance = self.input.param("pause_rebalance", False)
        self.crash_count = 0

        '''
        Create sequential: 0 - 10M
        Final Docs = 10M (0-10M, 10M seq items)
        '''
        self.create_perc = 100
        self.PrintStep("Step 2: Create %s items sequentially" % self.num_items)
        self.generate_docs(doc_ops=["create"],
                           create_start=0,
                           create_end=self.num_items)
        self.perform_load(validate_data=False)

        self.PrintStep("Step 2.1: Update %s RandonKey keys to create 50 percent fragmentation" % str(self.num_items))
        self.update_perc = 100
        self.generate_docs(doc_ops=["update"],
                           update_start=self.start,
                           update_end=self.end)
        self.perform_load(validate_data=False)

        self.PrintStep("Step 3: Create %s items sequentially" % self.num_items)
        self.generate_docs(doc_ops=["create"],
                           create_start=self.num_items,
                           create_end=self.num_items*2)
        self.perform_load(validate_data=False)

        self.PrintStep("Step 3.1: Update %s RandonKey keys to create 50 percent fragmentation" % str(self.num_items))
        self.update_perc = 100
        self.generate_docs(doc_ops=["update"],
                           update_start=self.start,
                           update_end=self.end)
        self.perform_load(validate_data=False)
        self.track_failures = False

        if self.index_nodes:
            self.drIndex.create_indexes()
            self.drIndex.build_indexes()
            self.drIndex.wait_for_indexes_online(self.log, self.drIndex.indexes)
            self.drIndex.start_query_load()

        if self.xdcr_remote_nodes > 0:
            self.drXDCR.create_remote_ref("magma_xdcr")
            for bucket in self.cluster.buckets:
                self.drXDCR.create_replication("magma_xdcr", bucket.name, bucket.name)

        self.rebl_nodes = self.input.param("rebl_nodes", 0)
        self.max_rebl_nodes = self.input.param("max_rebl_nodes", 1)
        self.doc_ops = self.input.param("doc_ops", "expiry").split(":")
        perc = 100/len(self.doc_ops)
        self.expiry_perc = perc
        self.create_perc = perc
        self.update_perc = perc
        self.delete_perc = perc
        self.read_perc = perc
        self.generate_docs(doc_ops=self.doc_ops,
                           read_start=0,
                           read_end=self.num_items,
                           create_start=self.num_items*2,
                           create_end=self.num_items*20,
                           update_start=self.num_items*2,
                           update_end=self.num_items*20,
                           expire_start=self.num_items*2,
                           expire_end=self.num_items*20
                           )
        self.ops_rate = self.input.param("rebl_ops_rate", self.ops_rate)
        self.perform_load(wait_for_load=False)
        while self.loop <= self.iterations:
            ###################################################################
            self.PrintStep("Step 4: Rebalance in with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            self.sleep(60, "Sleep for 60s for rebalance to start")

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()
            else:
                rebalance_task = self.abort_rebalance(rebalance_task, "kill_memcached")

            if rebalance_task is not None:
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            self.PrintStep("Step 5: Crash Magma/memc with Loading of docs")
            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 6: Rebalance Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=1)
            self.sleep(60, "Sleep for 60s for rebalance to start")

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()
            else:
                rebalance_task = self.abort_rebalance(rebalance_task, "kill_memcached")

            if rebalance_task is not None:
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            self.PrintStep("Step 7: Crash Magma/memc with Loading of docs")
            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 8: Rebalance In_Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=2, nodes_out=1,
                                            services=["kv"]*2)
            self.sleep(60, "Sleep for 60s for rebalance to start")

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()
            else:
                rebalance_task = self.abort_rebalance(rebalance_task, "kill_memcached")

            if rebalance_task is not None:
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            self.PrintStep("Step 9: Crash Magma/memc with Loading of docs")
            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 10: Swap with Loading of docs")

            rebalance_task = self.rebalance(nodes_in=1, nodes_out=1)
            self.sleep(60, "Sleep for 60s for rebalance to start")

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()
            else:
                rebalance_task = self.abort_rebalance(rebalance_task, "kill_memcached")

            if rebalance_task is not None:
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            self.PrintStep("Step 10: Crash Magma/memc with Loading of docs")
            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 11: Failover a node and RebalanceOut that node \
            with loading in parallel")

            # Chose node to failover
            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            # Failover Node
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(30)
            self.rest.monitorRebalance()

            # Rebalance out failed over node
            self.otpNodes = self.rest.node_statuses()
            self.rest.rebalance(otpNodes=[otpNode.id for otpNode in self.otpNodes],
                                ejectedNodes=[self.chosen[0].id])
            self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True),
                            msg="Rebalance failed")

            # Maintain nodes availability
            servs_out = [node for node in self.cluster.servers
                         if node.ip == self.chosen[0].ip]
            self.cluster.nodes_in_cluster = list(
                set(self.cluster.nodes_in_cluster) - set(servs_out))
            for server in servs_out:
                self.cluster.kv_nodes.remove(server)
            self.available_servers += servs_out
            self.print_stats()

            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 12: Failover a node and FullRecovery\
             that node")

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(30)
            self.rest.monitorRebalance()

            # Mark Node for full recovery
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id,
                                            recoveryType="full")

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [],
                retry_get_process_num=3000)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 13: Failover a node and DeltaRecovery that \
            node with loading in parallel")

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master,
                                                       howmany=1)

            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id,
                                                           graceful=True)
            self.sleep(30)
            self.rest.monitorRebalance()
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id,
                                            recoveryType="delta")

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [],
                retry_get_process_num=3000)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 14: Updating the bucket replica to 2")

            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.cluster.buckets)):
                bucket_helper.change_bucket_props(
                    self.cluster.buckets[i], replicaNumber=2)

            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            self.sleep(60, "Sleep for 60s for rebalance to start")

            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()

            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 15: Updating the bucket replica to 1")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.cluster.buckets)):
                bucket_helper.change_bucket_props(
                    self.cluster.buckets[i], replicaNumber=1)

            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster, [], [],
                retry_get_process_num=3000)
            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()

            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats()

            th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
            th.start()
            while self.crash_count < self.crashes:
                continue
            self.stop_crash = True
            th.join()

            ###################################################################
            self.PrintStep("Step 16: Start the entire process again")
            self.loop += 1
            if self.loop < self.iterations:
                self.sleep(10)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    nodes_cluster = self.cluster.nodes_in_cluster[:]
                    nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(
                        nodes_cluster,
                        int(len(self.cluster.nodes_in_cluster)
                            - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out,
                        retry_get_process_num=3000)

                    self.task_manager.get_task_result(
                        rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")
                    self.available_servers += servs_out
                    self.cluster.nodes_in_cluster = list(
                        set(self.cluster.nodes_in_cluster) - set(servs_out))
                    self.cluster.kv_nodes = list(
                        set(self.cluster.kv_nodes) - set(servs_out))
            self.print_stats()

        self.log.info("Volume Test Run Complete")
        self.doc_loading_tm.abortAllTasks()

    def ClusterOpsVolume(self):
        self.loop = 1
        self.skip_read_on_error = True
        self.suppress_error_table = True
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        def end_step_checks():
            self.print_stats()
            result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
            if result:
                self.stop_crash = True
                self.task.jython_task_manager.abort_all_tasks()
                self.assertFalse(
                    result,
                    "CRASH | CRITICAL | WARN messages found in cb_logs")

        self.loop = 0
        while self.loop < self.iterations:
            self.create_perc = 100
            self.PrintStep("Step 1: Create %s items sequentially" % self.num_items)
            self.generate_docs(doc_ops=["create"],
                               create_start=0,
                               create_end=self.num_items)
            self.perform_load(validate_data=False)

            self.PrintStep("Step 2: Update %s RandonKey keys to create 50 percent fragmentation" % str(self.num_items))
            self.update_perc = 100
            self.generate_docs(doc_ops=["update"],
                               update_start=self.start,
                               update_end=self.end)
            self.perform_load(validate_data=False)

            self.PrintStep("Step 3: Create %s items sequentially" % self.num_items)
            self.generate_docs(doc_ops=["create"],
                               create_start=self.num_items,
                               create_end=self.num_items*2)
            self.perform_load(validate_data=False)

            self.PrintStep("Step 4: Update %s RandonKey keys to create 50 percent fragmentation" % str(self.num_items))
            self.update_perc = 100
            self.generate_docs(doc_ops=["update"],
                               update_start=self.start,
                               update_end=self.end)
            self.perform_load(validate_data=False)
            self.ops_rate = self.input.param("rebl_ops_rate", self.ops_rate)
            ###################################################################
            if self.loop == 0:
                if self.index_nodes:
                    self.drIndex.create_indexes()
                    self.drIndex.build_indexes()
                    self.drIndex.wait_for_indexes_online(self.log, self.drIndex.indexes)
                    self.drIndex.start_query_load()

                if self.xdcr_remote_nodes > 0:
                    self.drXDCR.create_remote_ref("magma_xdcr")
                    for bucket in self.cluster.buckets:
                        self.drXDCR.create_replication("magma_xdcr", bucket.name, bucket.name)
            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 20M

            This Step:
            Create Random: 20 - 30M
            Delete Random: 10 - 20M
            Update Random: 0 - 10M
            Nodes In Cluster = 3 -> 4

            Final Docs = 30M (Random: 0-10M, 20-30M, Sequential: 0-10M)
            Nodes In Cluster = 4
            '''
            self.create_perc = 25
            self.update_perc = 25
            self.delete_perc = 25
            self.expiry_perc = 25
            self.read_perc = 25
            self.mutation_perc = self.input.param("mutation_perc", 100)
            self.generate_docs(doc_ops=["update", "delete", "read", "create"])
            tasks = self.perform_load(wait_for_load=False)
            self.rebl_services = self.input.param("rebl_services", ["kv"])
            self.rebl_nodes = self.input.param("rebl_nodes", 1)

            ###################################################################
            self.PrintStep("Step 5: Rebalance in with Loading of docs")

            rebalance_task = self.rebalance(nodes_in=self.rebl_nodes, nodes_out=0,
                                            services=self.rebl_services*self.rebl_nodes)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()
            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 20 - 30M

            This Step:
            Create Random: 30 - 40M
            Delete Random: 20 - 30M
            Update Random: 0 - 10M
            Nodes In Cluster = 4 -> 3

            Final Docs = 30M (Random: 0-10M, 30-40M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 6: Rebalance Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=self.rebl_nodes)

#             self.generate_docs(doc_ops=["update", "delete", "read", "create"])
#             tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 30 - 40M

            This Step:
            Create Random: 40 - 50M
            Delete Random: 30 - 40M
            Update Random: 0 - 10M
            Nodes In Cluster = 3 -> 4

            Final Docs = 30M (Random: 0-10M, 40-50M, Sequential: 0-10M)
            Nodes In Cluster = 4
            '''
            self.PrintStep("Step 7: Rebalance In_Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=self.rebl_nodes+1, nodes_out=self.rebl_nodes,
                                            services=self.rebl_services*(self.rebl_nodes+1))

#             self.generate_docs(doc_ops=["update", "delete", "read", "create"])
#             tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 40 - 50M

            This Step:
            Create Random: 50 - 60M
            Delete Random: 40 - 50M
            Update Random: 0 - 10M
            Nodes In Cluster = 4 -> 4 (SWAP)

            Final Docs = 30M (Random: 0-10M, 50-60M, Sequential: 0-10M)
            Nodes In Cluster = 4
            '''
            self.PrintStep("Step 8: Swap with Loading of docs")

            rebalance_task = self.rebalance(nodes_in=self.rebl_nodes, nodes_out=self.rebl_nodes,
                                            services=self.rebl_services*(self.rebl_nodes))

#             self.generate_docs(doc_ops=["update", "delete", "read", "create"])
#             tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 50 - 60M

            This Step:
            Create Random: 60 - 70M
            Delete Random: 50 - 60M
            Update Random: 0 - 10M
            Nodes In Cluster = 4 -> 3

            Final Docs = 30M (Random: 0-10M, 60-70M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 9: Failover %s node and RebalanceOut that node \
            with loading in parallel" % self.num_replicas)
            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(
                self.cluster.kv_nodes, self.cluster.buckets)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                get_and_compare_active_replica_data_set_all(
                    self.cluster.kv_nodes, self.cluster.buckets,
                    path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = random.sample(self.cluster.kv_nodes, self.num_replicas)
#             self.generate_docs(doc_ops=["update", "delete", "read", "create"])
#             tasks = self.perform_load(wait_for_load=False)

            # Mark Node for failover
            self.success_failed_over = True
            for node in self.chosen:
                failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                node.id = failover_node.id
                success_failed_over = self.rest.fail_over(failover_node.id,
                                                               graceful=True)
                self.success_failed_over = self.success_failed_over and success_failed_over
                self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                self.assertTrue(self.rest.monitorRebalance(), msg="Failover -> Rebalance failed")
            self.sleep(600, "Waiting for data to go in after failover.")

            self.nodes = self.rest.node_statuses()
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                                ejectedNodes=[node.id for node in self.chosen])
            self.assertTrue(self.rest.monitorRebalance(), msg="Rebalance failed")
            servs_out = []
            for failed_over in self.chosen:
                servs_out += [node for node in self.cluster.servers
                              if node.ip == failed_over.ip]
            self.cluster.nodes_in_cluster = list(
                set(self.cluster.nodes_in_cluster) - set(servs_out))
            self.available_servers += servs_out
            self.cluster.kv_nodes = list(set(self.cluster.kv_nodes) - set(servs_out))
            print "KV nodes in cluster: %s" % [server.ip for server in self.cluster.kv_nodes]
            print "CBAS nodes in cluster: %s" % [server.ip for server in self.cluster.cbas_nodes]
            print "INDEX nodes in cluster: %s" % [server.ip for server in self.cluster.index_nodes]
            print "FTS nodes in cluster: %s" % [server.ip for server in self.cluster.fts_nodes]
            print "QUERY nodes in cluster: %s" % [server.ip for server in self.cluster.query_nodes]
            print "EVENTING nodes in cluster: %s" % [server.ip for server in self.cluster.eventing_nodes]
            print "AVAILABLE nodes for cluster: %s" % [server.ip for server in self.available_servers]
            end_step_checks()

            self.bucket_util.compare_failovers_logs(
                self.cluster,
                prev_failover_stats,
                self.cluster.kv_nodes,
                self.cluster.buckets)

            self.bucket_util.data_analysis_active_replica_all(
                disk_active_dataset, disk_replica_dataset,
                self.cluster.servers[:self.nodes_in + self.nodes_init],
                self.cluster.buckets, path=None)
            nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
            self.bucket_util.vb_distribution_analysis(
                self.cluster,
                servers=nodes, buckets=self.cluster.buckets,
                num_replicas=self.num_replicas,
                std=std, total_vbuckets=self.cluster.vbuckets)

            ###################################################################
            extra_node_gone = self.num_replicas - 1
            if extra_node_gone > 0:
                self.PrintStep("Step 10: Rebalance in with Loading of docs")

                rebalance_task = self.rebalance(nodes_in=extra_node_gone,
                                                nodes_out=0,
                                                services=self.rebl_services*extra_node_gone)

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                end_step_checks()
            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 60 - 70M

            This Step:
            Create Random: 70 - 80M
            Delete Random: 60 - 70M
            Update Random: 0 - 10M
            Nodes In Cluster = 3 -> 3

            Final Docs = 30M (Random: 0-10M, 70-80M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 11: Failover a node and FullRecovery\
             that node")

            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(
                self.cluster.kv_nodes, self.cluster.buckets)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                get_and_compare_active_replica_data_set_all(
                    self.cluster.kv_nodes,
                    self.cluster.buckets,
                    path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = random.sample(self.cluster.kv_nodes, self.num_replicas)
#             self.generate_docs(doc_ops=["update", "delete", "read", "create"])
#             tasks = self.perform_load(wait_for_load=False)

            # Mark Node for failover
            self.success_failed_over = True
            for node in self.chosen:
                failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                node.id = failover_node.id
                success_failed_over = self.rest.fail_over(failover_node.id,
                                                               graceful=True)
                self.success_failed_over = self.success_failed_over and success_failed_over
                self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                self.assertTrue(self.rest.monitorRebalance(), msg="Failover -> Rebalance failed")
            self.sleep(600, "Waiting for data to go in after failover.")
            self.rest.monitorRebalance()

            # Mark Node for full recovery
            if self.success_failed_over:
                for node in self.chosen:
                    self.rest.set_recovery_type(otpNode=node.id,
                                                recoveryType="full")
            self.sleep(60, "Waiting for full recovery to finish and settle down cluster.")
            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster, [], [],
                retry_get_process_num=3000)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

            self.bucket_util.compare_failovers_logs(
                self.cluster,
                prev_failover_stats,
                self.cluster.kv_nodes + [self.cluster.master],
                self.cluster.buckets)

            self.bucket_util.data_analysis_active_replica_all(
                disk_active_dataset, disk_replica_dataset,
                self.cluster.kv_nodes + [self.cluster.master],
                self.cluster.buckets, path=None)
            self.bucket_util.vb_distribution_analysis(
                self.cluster,
                servers=self.cluster.kv_nodes + [self.cluster.master],
                buckets=self.cluster.buckets,
                num_replicas=self.num_replicas,
                std=std, total_vbuckets=self.cluster.vbuckets)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 70 - 80M

            This Step:
            Create Random: 80 - 90M
            Delete Random: 70 - 80M
            Update Random: 0 - 10M
            Nodes In Cluster = 3 -> 3

            Final Docs = 30M (Random: 0-10M, 80-90M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 12: Failover a node and DeltaRecovery that \
            node with loading in parallel")

            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

            prev_failover_stats = self.bucket_util.get_failovers_logs(
                self.cluster.kv_nodes + [self.cluster.master], self.cluster.buckets)

            disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                get_and_compare_active_replica_data_set_all(
                    self.cluster.kv_nodes + [self.cluster.master],
                    self.cluster.buckets,
                    path=None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = random.sample(self.cluster.kv_nodes, self.num_replicas)

#             self.generate_docs(doc_ops=["update", "delete", "read", "create"])
#             tasks = self.perform_load(wait_for_load=False)
            # Mark Node for failover
            self.success_failed_over = True
            for node in self.chosen:
                failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                node.id = failover_node.id
                success_failed_over = self.rest.fail_over(failover_node.id,
                                                               graceful=True)
                self.success_failed_over = self.success_failed_over and success_failed_over
                self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                self.assertTrue(self.rest.monitorRebalance(), msg="Failover -> Rebalance failed")
            self.sleep(600, "Waiting for data to go in after failover.")
            self.rest.monitorRebalance()

            # Mark Node for delta recovery
            if self.success_failed_over:
                for node in self.chosen:
                    self.rest.set_recovery_type(otpNode=node.id,
                                                recoveryType="delta")

            self.sleep(60, "Waiting for delta recovery to finish and settle down cluster.")
            rebalance_task = self.task.async_rebalance(
                self.cluster.nodes_in_cluster, [], [],
                retry_get_process_num=3000)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

            self.bucket_util.compare_failovers_logs(
                self.cluster,
                prev_failover_stats,
                self.cluster.kv_nodes + [self.cluster.master],
                self.cluster.buckets)

            self.bucket_util.data_analysis_active_replica_all(
                disk_active_dataset, disk_replica_dataset,
                self.cluster.kv_nodes + [self.cluster.master],
                self.cluster.buckets, path=None)
            self.bucket_util.vb_distribution_analysis(
                self.cluster,
                servers=self.cluster.kv_nodes + [self.cluster.master],
                buckets=self.cluster.buckets,
                num_replicas=self.num_replicas,
                std=std, total_vbuckets=self.cluster.vbuckets)

            ###################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 80 - 90M

            This Step:
            Create Random: 90 - 100M
            Delete Random: 80 - 90M
            Update Random: 0 - 10M
            Replica 1 - > 2

            Final Docs = 30M (Random: 0-10M, 90-100M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 13: Updating the bucket replica to %s" %
                           (self.num_replicas+1))

            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.cluster.buckets)):
                bucket_helper.change_bucket_props(
                    self.cluster.buckets[i], replicaNumber=self.num_replicas + 1)

            rebalance_task = self.rebalance(nodes_in=self.rebl_nodes, nodes_out=0,
                                            services=self.rebl_services*self.rebl_nodes)
#             self.generate_docs(doc_ops=["update", "delete", "read", "create"])
#             tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

            ####################################################################
            '''
            Existing:
            Sequential: 0 - 10M
            Random: 0 - 10M, 90 - 100M

            This Step:
            Create Random: 100 - 110M
            Delete Random: 90 - 100M
            Update Random: 0 - 10M
            Replica 2 - > 1

            Final Docs = 30M (Random: 0-10M, 100-110M, Sequential: 0-10M)
            Nodes In Cluster = 3
            '''
            self.PrintStep("Step 14: Updating the bucket replica to %s" %
                           self.num_replicas)
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.cluster.buckets)):
                bucket_helper.change_bucket_props(
                    self.cluster.buckets[i], replicaNumber=self.num_replicas)
#             self.generate_docs(doc_ops=["update", "delete", "read", "create"])
            rebalance_task = self.rebalance([], [])
#             tasks = self.perform_load(wait_for_load=False)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

        #######################################################################
            self.PrintStep("Step 15: Flush the bucket and \
            start the entire process again")
            self.wait_for_doc_load_completion(tasks)
            self.data_validation()
            self.loop += 1
            if self.loop < self.iterations:
                # Flush the bucket
                result = self.bucket_util.flush_all_buckets(self.cluster)
                self.assertTrue(result, "Flush bucket failed!")
                self.sleep(600)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    rebalance_task = self.rebalance(nodes_in=[], nodes_out=int(len(self.cluster.nodes_in_cluster)- self.nodes_init))
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")
            else:
                self.log.info("Volume Test Run Complete")
            self.init_doc_params()
