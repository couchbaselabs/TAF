'''
Created on 15-Apr-2021

@author: riteshagarwal
'''
import os
import random
import threading

from BucketLib.BucketOperations import BucketHelper
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from aGoodDoctor.bkrs import DoctorBKRS
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from pytests.basetestcase import BaseTestCase
# from bkrs import DoctorBKRS
from py_constants.cb_constants.CBServer import CbServer
from .cbas import CBASQueryLoad
from .cbas import DoctorCBAS
from cluster_utils.cluster_ready_functions import CBCluster
from bucket_utils.bucket_ready_functions import CollectionUtils
from shell_util.remote_connection import RemoteMachineShellConnection
from .workloads import default, nimbus, vector_load, quartz1, quartz2, quartz3,\
    quartz5, quartz4, quartz6, hotel_vector, siftBigANN
import struct
from index_utils.plasma_stats_util import PlasmaStatsUtil
from gsiLib.gsiHelper import GsiHelper
from .hostedEventing import DoctorEventing
import pprint
try:
    from .fts import DoctorFTS, FTSQueryLoad
except:
    pass
from membase.api.rest_client import RestConnection
from .n1ql import DoctorN1QL
from .n1ql import QueryLoad
from .opd import OPD
from .xdcr import DoctorXDCR


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
        self.eventing_nodes = self.input.param("eventing_nodes", 0)
        self.index_nodes = self.input.param("index_nodes", 0)
        self.backup_nodes = self.input.param("backup_nodes", 0)
        self.xdcr_remote_nodes = self.input.param("xdcr_remote_nodes", 0)
        self.num_indexes = self.input.param("num_indexes", 0)
        self.mutation_perc = 100
        self.doc_ops = self.input.param("doc_ops", "create")
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(':')

        self.threads_calculation()
        self.loader_tasks = list()
        self.rest = RestConnection(self.servers[0])
        self.op_type = self.input.param("op_type", "create")
        self.dgm = self.input.param("dgm", None)
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.num_buckets = self.input.param("num_buckets", 1)
        self.mutate = 0
        self.iterations = self.input.param("iterations", 10)
        self.step_iterations = self.input.param("step_iterations", 1)
        self.rollback = self.input.param("rollback", False)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.end_step = self.input.param("end_step", None)
        self.key_prefix = "test_docs-"
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
        self.val_type = self.input.param("val_type", "SimpleValue")
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.cursor_dropping_checkpoint = self.input.param(
            "cursor_dropping_checkpoint", None)
        self.index_timeout = self.input.param("index_timeout", 86400)
        self.assert_crashes_on_load = self.input.param("assert_crashes_on_load",
                                                       True)
        self.gtm = self.input.param("gtm", False)
        ##CDC Params
        self.bucket_history_retention_bytes = int(self.input.param("bucket_history_retention_bytes",0))
        self.bucket_history_retention_seconds = int(self.input.param("bucket_history_retention_seconds",0))

        self.ql = list()
        self.ftsQL = list()
        self.cbasQL = list()
        self.stop_run = False
        self.skip_init = self.input.param("skip_init", False)
        self.query_result = True

        self.esClient = None
        self.esHost = self.input.param("esHost", None)
        self.esAPIKey = self.input.param("esAPIKey", None)
        if self.esHost:
            self.esHost = "http://" + self.esHost + ":9200"
        if self.esAPIKey:
            self.esAPIKey = "".join(self.esAPIKey.split(","))
        
        self.load_defn = list()
        nimbus = self.input.param("nimbus", False)
        quartz = self.input.param("quartz", False)
        expiry = self.input.param("expiry", False)
        self.load_defn.append(default)
        if nimbus:
            self.load_defn = list()
            self.load_defn.append(nimbus)

        if quartz:
            self.load_defn = list()
            self.load_defn.extend([quartz1, quartz2, quartz3, quartz4, quartz5, quartz6])

        if expiry:
            for load in self.load_defn:
                load["pattern"] = [0, 80, 0, 0, 20]
                load["load_type"] = ["read", "expiry"]

        self.siftFileName = None
        if self.vector:
            self.load_defn = list()
            if self.index_nodes > 0:
                if self.val_type == "Hotel":
                    self.load_defn.append(hotel_vector)
                if self.val_type == "siftBigANN":
                    self.load_defn.append(siftBigANN)
            else:
                self.load_defn.append(vector_load)

        #######################################################################
        self.PrintStep("Step 1: Create a %s node cluster" % self.nodes_init)
        if self.nodes_init > 1 and len(self.cluster.nodes_in_cluster) < self.nodes_init:
            services = list()
            nodes_init = self.cluster.servers[1:self.nodes_init]
            if self.services_init:
                for service in self.services_init.split("-"):
                    services.append(service.replace(":", ","))
            self.task.rebalance(self.cluster, nodes_init, [], services=services*len(nodes_init),
                                validate_bucket_ranking=False)
        self.available_servers = self.cluster.servers[len(self.cluster.nodes_in_cluster):]
        self.cluster_util.set_metadata_purge_interval(self.cluster.master,
                                                      interval=self.bucket_purge_interval)
        if self.xdcr_remote_nodes > 0:
            self.assertTrue(self.xdcr_remote_nodes <= len(self.available_servers),
                            "Only {0} nodes available, cannot create XDCR remote with {1} nodes".format(
                                len(self.available_servers), self.xdcr_remote_nodes))
            remote_nodes = self.available_servers[0:self.xdcr_remote_nodes]
            self.available_servers = self.available_servers[self.xdcr_remote_nodes:]
            self.PrintStep("Step 1*: Create a %s node XDCR remote cluster" % self.xdcr_remote_nodes)
            self.xdcr_remote_cluster = CBCluster(
                name="remote", servers=remote_nodes,
                vbuckets=self.vbuckets or CbServer.total_vbuckets)
            self.xdcr_remote_cluster.nodes_in_cluster.append(
                self.xdcr_remote_cluster.master)
            self._initialize_nodes(self.task,
                                   self.xdcr_remote_cluster,
                                   self.disabled_consistent_view,
                                   self.rebalanceIndexWaitingDisabled,
                                   self.rebalanceIndexPausingDisabled,
                                   self.maxParallelIndexers,
                                   self.maxParallelReplicaIndexers,
                                   self.port,
                                   self.quota_percent)

            self.task.rebalance(self.xdcr_remote_cluster, remote_nodes[1:], [],
                                validate_bucket_ranking=False)
        #######################################################################
        self.PrintStep("Step 2: Create required buckets and collections.")
        if self.num_buckets > 10:
            self.bucket_util.change_max_buckets(self.cluster.master,
                                                self.num_buckets)
        if not self.skip_init:
            self.create_required_buckets(self.cluster)
        else:
            for i, bucket in enumerate(self.cluster.buckets):
                bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                num_clients = self.input.param("clients_per_db",
                                               min(5, bucket.loadDefn.get("collections")))
                SiriusCouchbaseLoader.create_clients_in_pool(
                    self.cluster.master, self.cluster.master.rest_username,
                    self.cluster.master.rest_password,
                    bucket.name, req_clients=num_clients)
                self.create_sdk_client_pool(self.cluster, self.cluster.buckets, 1)
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                        continue
                    if bucket.loadDefn.get("collections") > 0:
                        self.collection_prefix = self.input.param("collection_prefix",
                                                                  "VolumeCollection")

                        for i in range(bucket.loadDefn.get("collections")):
                            collection_name = self.collection_prefix + str(i)
                            collection_spec = {"name": collection_name}
                            CollectionUtils.create_collection_object(bucket, scope, collection_spec)

        coll_id = self.input.param("collection_id", False)
        if coll_id and self.val_type == "siftBigANN":
            coll_id = coll_id.split(",")
            for bucket in self.cluster.buckets:
                bucket.loadDefn["collections_defn"] = [defn for defn in bucket.loadDefn["collections_defn"] if defn.get("collection_id") in coll_id]
        # if self.esHost and self.esAPIKey:
        #     self.esClient = EsClient(self.esHost, self.esAPIKey)
        #     self.esClient.initializeSDK()
        #     if not self.skip_init:
        #         for bucket in self.cluster.buckets:
        #             for scope in bucket.scopes.keys():
        #                 if scope == CbServer.system_scope:
        #                     continue
        #                 for collection in bucket.scopes[scope].collections.keys():
        #                     if scope == CbServer.system_scope:
        #                         continue
        #                     if collection == "_default" and scope == "_default":
        #                         continue 
        #                     self.esClient.deleteESIndex(collection.lower())
        #                     self.esClient.createESIndex(collection.lower())
        if self.xdcr_remote_nodes > 0:
            self.PrintStep("Step 2*: Create required buckets and collections on XDCR remote.")
            self.create_required_buckets(cluster=self.xdcr_remote_cluster)
            # Increase number of collections to increase number of replicated docs
            self.drXDCR = DoctorXDCR(self.cluster, self.xdcr_remote_cluster)

        self.rest = RestConnection(self.cluster.master)
        self.af_timeout = self.input.param("af_timeout", 600)
        self.af_enable = self.input.param("af_enable", False)
        storageModeGSI = self.input.param("storageModeGSI", "plasma")
        self.rest.set_indexer_storage_mode(storageModeGSI)
        self.assertTrue(
            self.rest.update_autofailover_settings(self.af_enable,
                                                   self.af_timeout),
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
        if self.cbas_nodes>0 and self.cbas_nodes > len(self.cluster.cbas_nodes):
            self.rest.set_service_mem_quota({CbServer.Settings.CBAS_MEM_QUOTA:
                                             int(server.mcdMemoryReserved - 100
                                                 )})
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster,
                                self.servers[nodes:nodes+self.cbas_nodes], [],
                                services=["cbas"]*self.cbas_nodes,
                                validate_bucket_ranking=False)

            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in self.cluster.cbas_nodes]

        if self.backup_nodes:
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster,
                                self.servers[nodes:nodes+self.backup_nodes], [],
                                services=["backup"]*self.backup_nodes,
                                validate_bucket_ranking=False)

        if self.index_nodes>0 and self.index_nodes > len(self.cluster.index_nodes):
            self.indexer_mem_quota = self.input.param("indexer_mem_quota", None)
            self.enableShardAffinity = self.input.param("enableShardAffinity", True)
            if self.indexer_mem_quota:
                self.rest.set_service_mem_quota({CbServer.Settings.INDEX_MEM_QUOTA:
                                             int(self.indexer_mem_quota
                                                 )})
            else:
                self.rest.set_service_mem_quota({CbServer.Settings.INDEX_MEM_QUOTA:
                                             int(server.mcdMemoryReserved - 100
                                                 )})

            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster,
                                self.servers[nodes:nodes+self.index_nodes], [],
                                services=["index,n1ql"]*self.index_nodes,
                                validate_bucket_ranking=False)
            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in self.cluster.index_nodes]
            _ = self.rest.set_indexer_params(redistributeIndexes='true', enableShardAffinity='true',
                                             storageMode=storageModeGSI) \
            if self.enableShardAffinity else self.rest.set_indexer_params(redistributeIndexes='true',
                                                                          storageMode=storageModeGSI)
        self.gsi_rest = GsiHelper(self.cluster.index_nodes[0], self.log)
        enableInMemoryCompression = self.input.param("enableInMemoryCompression", False)
        if enableInMemoryCompression is False:
            self.sleep(0, "sleep before setting indexer params")
            self.gsi_rest.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": False})
        reranking = self.input.param("reranking", False)
        if reranking is False:
            self.sleep(0, "sleep before setting indexer params")
            self.gsi_rest.set_index_settings({"indexer.scan.vector.rerank_factor": 0})
        # self.sleep(10, "sleep  after setting indexer params")
        if self.fts_nodes>0 and self.fts_nodes > len(self.cluster.fts_nodes):
            self.rest.set_service_mem_quota({CbServer.Settings.FTS_MEM_QUOTA:
                                             int(server.mcdMemoryReserved*0.7
                                                 )})
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster,
                                self.servers[nodes:nodes+self.fts_nodes], [],
                                services=["fts"]*self.fts_nodes,
                                validate_bucket_ranking=False)
            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in self.cluster.fts_nodes]
        if self.eventing_nodes>0 and self.eventing_nodes > len(self.cluster.eventing_nodes):
            self.rest.set_service_mem_quota({CbServer.Settings.EVENTING_MEM_QUOTA:
                                             int(server.mcdMemoryReserved*0.7
                                                 )})
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster,
                                self.servers[nodes:nodes+self.eventing_nodes], [],
                                services=["eventing"]*self.eventing_nodes,
                                validate_bucket_ranking=False)
            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in self.cluster.eventing_nodes]

        if self.cluster.backup_nodes:
            self.drBackup = DoctorBKRS(self.cluster)
        if self.cluster.index_nodes:
            self.drIndex = DoctorN1QL(self.bucket_util)
        if self.cluster.cbas_nodes:
            self.drCBAS = DoctorCBAS(self.bucket_util)
        if self.cluster.fts_nodes:
            self.drFTS = DoctorFTS(self.bucket_util)

        print (self.available_servers)
        self.writer_threads = self.input.param("writer_threads", "disk_io_optimized")
        self.reader_threads = self.input.param("reader_threads", "disk_io_optimized")
        self.storage_threads = self.input.param("storage_threads", 40)
        ClusterRestAPI(self.cluster.master).manage_cluster_connections(
                                num_writer_threads=self.writer_threads,
                                num_reader_threads=self.reader_threads,
                                num_storage_threads=self.storage_threads)
        self.stop_rebalance = self.input.param("pause_rebalance", False)
        self.log_query_failures = True

    def tearDown(self):
        for task in self.loader_tasks:
            self.doc_loading_tm.stop_task(task)
        self.check_dump_thread = False
        self.mutations = False
        self.stop_run = True
        self.stop_crash = True
        if self.cluster.index_nodes:
            self.drIndex.discharge_N1QL()
        for task in self.ql:
            task.stop_query_load()
        for task in self.ftsQL:
            task.stop_query_load()
        for task in self.cbasQL:
            task.stop_query_load()
        BaseTestCase.tearDown(self)

    def testKvRangeScan(self):
        self.create_perc = 100
        self.PrintStep("Step 1: Create %s items: %s" % (self.num_items, self.key_type))
        self.generate_docs(doc_ops=["create"],
                           create_start=0,
                           create_end=self.num_items)
        self.perform_load(validate_data=False)
        self.drN1QL = DoctorN1QL(self.cluster, self.bucket_util,
                                      self.num_indexes, num_query=5, query_without_index=True)

        th = threading.Thread(target=self.drN1QL.run_concurrent_queries,
                              kwargs=dict(num_queries=5))
        th.start()

        self.doc_ops = self.input.param("doc_ops", "create").split(":")
        perc = 100/len(self.doc_ops)
        self.expiry_perc = perc
        self.create_perc = perc
        self.update_perc = perc
        self.delete_perc = perc
        self.read_perc = perc
        self.generate_docs(doc_ops=self.doc_ops,
                           read_start=0,
                           read_end=self.num_items,
                           create_start=self.num_items,
                           create_end=self.num_items*10,
                           update_start=0,
                           update_end=self.num_items*10,
                           delete_start=self.num_items//2,
                           delete_end=self.num_items,
                           expire_start=self.num_items,
                           expire_end=self.num_items*10
                           )
        self.perform_load(wait_for_load=False, validate_data=False)
        crash_th = threading.Thread(target=self.crash_memcached,
                                  kwargs={"graceful": False})
        crash_th.start()
        self.doc_loading_tm.getAllTaskResult()

        self.stop_crash = True
        crash_th.join()
        self.drN1QL.discharge_N1QL()
        th.join()
        self.PrintStep("testKvRangeScan ends")

    def SteadyStateVolume(self):
        self.initial_setup()
        self.loop = 1
        self.doc_ops = self.input.param("doc_ops", "expiry").split(":")
        perc = 100/len(self.doc_ops)
        self.expiry_perc = perc
        self.create_perc = perc
        self.update_perc = perc
        self.delete_perc = perc
        self.read_perc = perc
        self.mutation_perc = self.input.param("mutation_perc", 100)
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
        # self.stop_stats = True
        # stat_th.join()

        # Starting the backup here.
        if self.backup_nodes > 0:
            self.restore_timeout = self.input.param("restore_timeout", 12*60*60)
            archive = os.path.join(self.cluster.backup_nodes[0].data_path, "bkrs")
            repo = "magma"
            self.drBackup.configure_backup(archive, repo, [], [])
            self.drBackup.trigger_backup(archive, repo)
            items = self.bucket_util.get_buckets_item_count(
                self.cluster,
                self.cluster.buckets[0].name)
            self.bucket_util.flush_all_buckets(self.cluster)
            self.drBackup.trigger_restore(archive, repo)
            result = self.drBackup.monitor_restore(self.bucket_util, items, timeout=self.restore_timeout)
            self.assertTrue(result, "Restore failed")

    def initial_setup(self):
        cpu_monitor = threading.Thread(target=self.print_stats_loop,
                                       kwargs={"cluster": self.cluster})
        cpu_monitor.start()

        self.monitor_query_status()
        self.skip_read_on_error = True
        self.suppress_error_table = True
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        '''
        Create sequential: 0 - 10M
        Final Docs = 10M (0-10M, 10M seq items)
        '''
        if self.vector and self.cluster.eventing_nodes:
            self.drEventing = DoctorEventing(self.bucket_util)
            self.drEventing.create_eventing_function(self.cluster, file="pytests/aGoodDoctor/vector_xattr.json")
            self.drEventing.lifecycle_operation_for_all_functions(self.cluster, "deploy", "deployed")

        if self.val_type == "siftBigANN":
            self.siftBigANN_load()
        else:
            self.normal_load()
        if self.cluster.cbas_nodes:
            self.drCBAS.create_datasets(self.cluster.buckets)
            self.drCBAS.create_indexes(self.cluster.buckets)
            result = self.drCBAS.wait_for_ingestion(
                self.cluster.buckets, self.index_timeout)
            self.assertTrue(result, "CBAS ingestion couldn't complete in time: %s" % self.index_timeout)
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("cbasQPS", 0) > 0:
                    ql = CBASQueryLoad(bucket)
                    ql.start_query_load()
                    self.cbasQL.append(ql)

        if self.cluster.index_nodes:
            self.combinational = self.input.param("combinational", False)
            self.drIndex.create_indexes(self.cluster.buckets, base64=self.base64, xattr=self.xattr,
                                        combinational=self.combinational)
            self.drIndex.build_indexes(self.cluster, self.cluster.buckets, wait=True)
            self.check_index_pending_mutations(self.cluster)
            self.end_step_checks(" after initial index build is completed")
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("2iQPS", 0) > 0:
                    ql = QueryLoad(bucket, self.mockVector,
                                   validate_item_count=self.input.param("validate_query_results", True),
                                   esClient=self.esClient, log_fail=self.log_query_failures)
                    ql.start_query_load()
                    self.ql.append(ql)
            self.drIndex.start_update_stats(self.cluster)
            self.drIndex.start_index_stats(self.cluster)

        if self.cluster.fts_nodes:
            self.drFTS.create_fts_indexes(self.cluster, dims=self.dim,
                                          _type=self.fts_index_type)
            status = self.drFTS.wait_for_fts_index_online(self.cluster,
                                                          self.index_timeout)
            self.assertTrue(status, "FTS index build failed.")
            self.sleep(300, "Wait for memory to be released after FTS index build.")
            for bucket in self.cluster.buckets:
                if bucket.loadDefn.get("ftsQPS", 0) > 0:
                    ql = FTSQueryLoad(self.cluster, bucket, self.esClient,
                                  self.mockVector, self.dim, self.base64)
                    ql.start_query_load()
                    self.ftsQL.append(ql)

        if self.xdcr_remote_nodes > 0:
            self.drXDCR.create_remote_ref("magma_xdcr")
            for bucket in self.cluster.buckets:
                self.drXDCR.create_replication("magma_xdcr", bucket.name, bucket.name)

        self.PrintStep("Running Query workload for {} with NO mutations".format(
            self.input.param("steady_state_workload_sleep", 300)))
        self.sleep(self.input.param("steady_state_workload_sleep", 300))
        self.end_step_checks(" queries withOUT mutations")

        if self.rollback:
            self.trigger_rollback()
        
        if self.val_type == "siftBigANN":
            self.mutations = True
            self.mutation_th = threading.Thread(target=self.sift_mutations)
            self.mutation_th.start()

            self.PrintStep("Running Query workload for {} during mutations".format(
                self.input.param("mutations_only_workload_sleep", 300)))
            self.restart_query_load(self.cluster, 0)
            self.sleep(self.input.param("mutations_only_workload_sleep", 300))
            self.end_step_checks(" queries with mutations")
        else:
            self.mutations = True
            self.mutation_th = threading.Thread(target=self.normal_mutations)
            self.mutation_th.start()

    def test_rebalance(self):
        self.initial_setup()

        self.loop = 1
        self.rebl_nodes = self.input.param("rebl_nodes", 0)
        self.max_rebl_nodes = self.input.param("max_rebl_nodes", 1)

        self.rebl_services = self.input.param("rebl_services", "fts").split("-")
        self.sleep(30)
        self.mutation_perc = self.input.param("mutation_perc", 100)
        while self.loop <= self.iterations:
            self.rebl_nodes += 1
            if self.rebl_nodes > self.max_rebl_nodes:
                self.rebl_nodes = 1
            for service in self.rebl_services:
                ###################################################################
                self.PrintStep("Step 6.{}: Rebalance SWAP with Loading of docs".
                               format(self.loop))
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes,
                                                nodes_out=self.rebl_nodes,
                                                services=[service]*self.rebl_nodes,
                                                retry_get_process_num=3000)
                self.sleep(60, "Sleep for 60s for rebalance to start")
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats(self.cluster)
                self.sleep(10, "Sleep for 60s after rebalance")

                self.PrintStep("Step 4.{}: Rebalance IN with Loading of docs".
                               format(self.loop))
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes,
                                                nodes_out=0,
                                                services=[service]*self.rebl_nodes,
                                                retry_get_process_num=3000)
                self.sleep(60, "Sleep for 60s for rebalance to start")
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats(self.cluster)
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
                self.print_stats(self.cluster)
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
                self.print_stats(self.cluster)
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
                self.print_stats(self.cluster)
                self.sleep(10, "Sleep for 60s after rebalance")

                self.loop += 1
            if self.track_failures:
                self.data_validation(self.cluster)

    def SystemTestMagma(self):
        self.initial_setup()

        self.loop = 1
        self.key_type = "RandomKey"
        self.crash_count = 0
        self.rebl_nodes = self.input.param("rebl_nodes", 0)
        self.max_rebl_nodes = self.input.param("max_rebl_nodes", 1)

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
            self.print_stats(self.cluster)

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
            self.print_stats(self.cluster)

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
            self.print_stats(self.cluster)

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
            self.print_stats(self.cluster)

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
            nodes = [node for node in self.cluster.kv_nodes if node.ip != self.cluster.master.ip]
            self.chosen = random.sample(nodes, self.num_replicas)

            # Mark Node for failover
            self.success_failed_over = True
            for node in self.chosen:
                failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                node.id = failover_node.id
                success_failed_over = self.rest.fail_over(failover_node.id,
                                                          graceful=True)
                self.success_failed_over = self.success_failed_over and success_failed_over
                self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
            self.sleep(600, "Waiting for data to go in after failover.")

            # Rebalance out failed over node
            self.nodes = self.rest.node_statuses()
            self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                                ejectedNodes=[node.id for node in self.chosen])
            self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Rebalance failed")
            servs_out = []
            for failed_over in self.chosen:
                servs_out += [node for node in self.cluster.servers
                              if node.ip == failed_over.ip]
                self.cluster.kv_nodes.remove(failed_over)
            self.available_servers += servs_out
            self.print_stats(self.cluster)

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
            self.chosen = random.sample(self.cluster.kv_nodes, self.num_replicas)

            # Mark Node for failover
            self.success_failed_over = True
            for node in self.chosen:
                failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                node.id = failover_node.id
                success_failed_over = self.rest.fail_over(failover_node.id,
                                                               graceful=True)
                self.success_failed_over = self.success_failed_over and success_failed_over
                self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
            self.sleep(600, "Waiting for data to go in after failover.")
            self.rest.monitorRebalance(progress_count=50000)

            # Mark Node for full recovery
            if self.success_failed_over:
                for node in self.chosen:
                    self.rest.set_recovery_type(otpNode=node.id,
                                                recoveryType="full")

            rebalance_task = self.task.async_rebalance(
                self.cluster, [], [],
                retry_get_process_num=3000,
                validate_bucket_ranking=False)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats(self.cluster)

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
                self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
            self.sleep(600, "Waiting for data to go in after failover.")
            self.rest.monitorRebalance(progress_count=50000)

            # Mark Node for delta recovery
            if self.success_failed_over:
                for node in self.chosen:
                    self.rest.set_recovery_type(otpNode=node.id,
                                                recoveryType="delta")

            self.sleep(60, "Waiting for delta recovery to finish and settle down cluster.")
            rebalance_task = self.task.async_rebalance(
                self.cluster, [], [],
                retry_get_process_num=3000,
                validate_bucket_ranking=False)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats(self.cluster)

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
            self.print_stats(self.cluster)

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
                self.cluster, [], [],
                retry_get_process_num=3000,
                validate_bucket_ranking=False)
            if self.stop_rebalance:
                rebalance_task = self.pause_rebalance()

            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats(self.cluster)

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
                if len(self.cluster.kv_nodes) > self.nodes_init:
                    rebalance_task = self.rebalance(nodes_in=[], nodes_out=int(len(self.cluster.kv_nodes) + 1 - self.nodes_init),
                                                    services=["kv"])
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")
            self.print_stats(self.cluster)

        self.log.info("Volume Test Run Complete")
        self.mutations = False
        self.doc_loading_tm.abortAllTasks()

    def ClusterOpsVolume(self):
        self.loop = 1

        self.initial_setup()


        self.loop = 0
        while self.loop < self.iterations:
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
            self.rebl_services = self.input.param("rebl_services", "kv").split("-")
            self.rebl_nodes = self.input.param("rebl_nodes", 1)
            for service in self.rebl_services:
                self.PrintStep("Step 5: Rebalance in of {} node with Loading of docs".format(service))

                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes, nodes_out=0,
                                                services=[service]*self.rebl_nodes)

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.end_step_checks()
    
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

                self.PrintStep("Step 6: Rebalance Out of {} node with Loading of docs".format(service))
                rebalance_task = self.rebalance(nodes_in=0, nodes_out=self.rebl_nodes,
                                                services=[service]*self.rebl_nodes)

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.end_step_checks()

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

                self.PrintStep("Step 9: Rebalance In_Out of {} nodes with Loading of docs".format(service))
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes+1, nodes_out=self.rebl_nodes,
                                                services=[service]*(self.rebl_nodes+1))

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.end_step_checks()

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

                self.PrintStep("Step 10: Swap Rebalance of {} Nodes with Loading of docs".format(service))

                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes, nodes_out=self.rebl_nodes,
                                                services=[service]*(self.rebl_nodes))

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.end_step_checks()

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
                self.PrintStep("Step 11: Failover %s node and RebalanceOut that node \
                with loading in parallel" % self.num_replicas)
                graceful = False
                failover_nodes = self.cluster.kv_nodes
                if service == "fts":
                    failover_nodes = self.cluster.fts_nodes
                elif service == "cbas":
                    failover_nodes = self.cluster.cbas_nodes
                elif service == "index":
                    failover_nodes = self.cluster.index_nodes
                elif service == "query":
                    failover_nodes = self.cluster.query_nodes
                elif service == "eventing":
                    failover_nodes = self.cluster.eventing_nodes
                nodes = [node for node in failover_nodes if node.ip != self.cluster.master.ip]
                self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
                std = self.std_vbucket_dist or 1.0

                if service == "kv":
                    graceful = True
                    prev_failover_stats = self.bucket_util.get_failovers_logs(
                        nodes, self.cluster.buckets)

                    disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                        get_and_compare_active_replica_data_set_all(
                            self.cluster.kv_nodes, self.cluster.buckets,
                            path=None)

                self.rest = RestConnection(self.cluster.master)
                self.chosen = random.sample(nodes, self.num_replicas)

                # Mark Node for failover
                self.success_failed_over = True
                for node in self.chosen:
                    failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                    node.id = failover_node.id
                    success_failed_over = self.rest.fail_over(failover_node.id,
                                                              graceful=graceful)
                    self.success_failed_over = self.success_failed_over and success_failed_over
                    self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                    self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
                self.sleep(600, "Waiting for data to go in after failover.")

                self.nodes = self.rest.node_statuses()
                self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                                    ejectedNodes=[node.id for node in self.chosen])
                self.assertTrue(self.rest.monitorRebalance(progress_count=500000), msg="Rebalance failed")
                servs_out = []
                for failed_over in self.chosen:
                    servs_out += [node for node in self.cluster.servers
                                  if node.ip == failed_over.ip]
                    if service == "fts":
                        self.cluster.fts_nodes.remove(failed_over)
                    elif service == "cbas":
                        self.cluster.cbas_nodes.remove(failed_over)
                    elif service == "index":
                        self.cluster.index_nodes.remove(failed_over)
                    elif service == "query":
                        self.cluster.query_nodes.remove(failed_over)
                    elif service == "eventing":
                        self.cluster.eventing_nodes.remove(failed_over)
                    elif service == "kv":
                        self.cluster.kv_nodes.remove(failed_over)
                self.available_servers += servs_out
                print("KV nodes in cluster: %s" % [server.ip for server in self.cluster.kv_nodes])
                print("CBAS nodes in cluster: %s" % [server.ip for server in self.cluster.cbas_nodes])
                print("INDEX nodes in cluster: %s" % [server.ip for server in self.cluster.index_nodes])
                print("FTS nodes in cluster: %s" % [server.ip for server in self.cluster.fts_nodes])
                print("QUERY nodes in cluster: %s" % [server.ip for server in self.cluster.query_nodes])
                print("EVENTING nodes in cluster: %s" % [server.ip for server in self.cluster.eventing_nodes])
                print("AVAILABLE nodes for cluster: %s" % [server.ip for server in self.available_servers])
                self.end_step_checks()
                if service == "kv":
                    self.bucket_util.compare_failovers_logs(
                        self.cluster,
                        prev_failover_stats,
                        nodes,
                        self.cluster.buckets)

                    self.bucket_util.data_analysis_active_replica_all(
                        disk_active_dataset, disk_replica_dataset,
                        self.cluster.servers[:self.nodes_in + self.nodes_init],
                        self.cluster.buckets, path=None)
                    nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
                    self.bucket_util.vb_distribution_analysis(
                        self.cluster,
                        servers=self.cluster.kv_nodes,
                        buckets=self.cluster.buckets,
                        num_replicas=self.num_replicas,
                        std=std)
        
                ###################################################################
                extra_node_gone = self.num_replicas - 1
                if extra_node_gone > 0:

                    self.PrintStep("Step 12: Rebalance in of KV Node with Loading of docs")

                    rebalance_task = self.rebalance(nodes_in=extra_node_gone,
                                                    nodes_out=0,
                                                    services=self.rebl_services*extra_node_gone)

                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")
                    self.end_step_checks()

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
                self.PrintStep("Step 13: Failover a node and FullRecovery\
                 that node")
                failover_nodes = self.cluster.kv_nodes
                if service == "fts":
                    failover_nodes = self.cluster.fts_nodes
                elif service == "cbas":
                    failover_nodes = self.cluster.cbas_nodes
                elif service == "index":
                    failover_nodes = self.cluster.index_nodes
                elif service == "query":
                    failover_nodes = self.cluster.query_nodes
                elif service == "eventing":
                    failover_nodes = self.cluster.eventing_nodes
                nodes = [node for node in failover_nodes if node.ip != self.cluster.master.ip]
                self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
                std = self.std_vbucket_dist or 1.0
                if service == "kv":
                    prev_failover_stats = self.bucket_util.get_failovers_logs(
                        nodes, self.cluster.buckets)

                    disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                        get_and_compare_active_replica_data_set_all(
                            self.cluster.kv_nodes,
                            self.cluster.buckets,
                            path=None)

                self.rest = RestConnection(self.cluster.master)
                self.chosen = random.sample(nodes, self.num_replicas)

                # Mark Node for failover
                self.success_failed_over = True
                for node in self.chosen:
                    failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                    node.id = failover_node.id
                    success_failed_over = self.rest.fail_over(failover_node.id,
                                                              graceful=graceful)
                    self.success_failed_over = self.success_failed_over and success_failed_over
                    self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                    self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
                self.sleep(600, "Waiting for data to go in after failover.")
                self.rest.monitorRebalance(progress_count=50000)

                # Mark Node for full recovery
                if self.success_failed_over:
                    for node in self.chosen:
                        self.rest.set_recovery_type(otpNode=node.id,
                                                    recoveryType="full")
                self.sleep(60, "Waiting for full recovery to finish and settle down cluster.")
                rebalance_task = self.task.async_rebalance(
                    self.cluster, [], [],
                    retry_get_process_num=3000,
                    validate_bucket_ranking=False)

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.end_step_checks()
                if service == "kv":
                    self.bucket_util.compare_failovers_logs(
                        self.cluster,
                        prev_failover_stats,
                        nodes,
                        self.cluster.buckets)

                    self.bucket_util.data_analysis_active_replica_all(
                        disk_active_dataset, disk_replica_dataset,
                        self.cluster.kv_nodes,
                        self.cluster.buckets, path=None)
                    self.bucket_util.vb_distribution_analysis(
                        self.cluster,
                        servers=self.cluster.kv_nodes,
                        buckets=self.cluster.buckets,
                        num_replicas=self.num_replicas,
                        std=std)

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
                nodes = [node for node in self.cluster.kv_nodes if node.ip != self.cluster.master.ip]
                self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
                std = self.std_vbucket_dist or 1.0

                prev_failover_stats = self.bucket_util.get_failovers_logs(
                    nodes, self.cluster.buckets)

                disk_replica_dataset, disk_active_dataset = self.bucket_util.\
                    get_and_compare_active_replica_data_set_all(
                        self.cluster.kv_nodes,
                        self.cluster.buckets,
                        path=None)

                self.rest = RestConnection(self.cluster.master)
                self.nodes = self.cluster_util.get_nodes(self.cluster.master)
                self.chosen = random.sample(nodes, self.num_replicas)

                # Mark Node for failover
                self.success_failed_over = True
                for node in self.chosen:
                    failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                    node.id = failover_node.id
                    success_failed_over = self.rest.fail_over(failover_node.id,
                                                                   graceful=True)
                    self.success_failed_over = self.success_failed_over and success_failed_over
                    self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                    self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
                self.sleep(600, "Waiting for data to go in after failover.")
                self.rest.monitorRebalance(progress_count=50000)

                # Mark Node for delta recovery
                if self.success_failed_over:
                    for node in self.chosen:
                        self.rest.set_recovery_type(otpNode=node.id,
                                                    recoveryType="delta")

                self.sleep(60, "Waiting for delta recovery to finish and settle down cluster.")
                rebalance_task = self.task.async_rebalance(
                    self.cluster, [], [],
                    retry_get_process_num=3000,
                    validate_bucket_ranking=False)
                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.end_step_checks()

                self.bucket_util.compare_failovers_logs(
                    self.cluster,
                    prev_failover_stats,
                    nodes,
                    self.cluster.buckets)

                self.bucket_util.data_analysis_active_replica_all(
                    disk_active_dataset, disk_replica_dataset,
                    self.cluster.kv_nodes,
                    self.cluster.buckets, path=None)
                self.bucket_util.vb_distribution_analysis(
                    self.cluster,
                    servers=self.cluster.kv_nodes,
                    buckets=self.cluster.buckets,
                    num_replicas=self.num_replicas,
                    std=std)

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

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.end_step_checks()

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
                rebalance_task = self.rebalance([], [])

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.end_step_checks()


            #######################################################################
                self.loop += 1
                if self.loop < self.iterations:
                    self.sleep(600)
                    if len(self.cluster.kv_nodes) > self.nodes_init:
                        rebalance_task = self.rebalance(nodes_in=[], nodes_out=int(len(self.cluster.kv_nodes) + 1 - self.nodes_init),
                                                        services=["kv"])
                        self.task.jython_task_manager.get_task_result(rebalance_task)
                        self.assertTrue(rebalance_task.result, "Rebalance Failed")
                else:
                    self.log.info("Volume Test Run Complete")
                self.init_doc_params()

        self.mutations = False
        self.mutation_th.join()
        if self.val_type != "siftBigANN" and self.track_failures:
            tasks = list()
            self.doc_loading_tm.getAllTaskResult()
            for task in tasks:
                self.assertTrue(task.result, "Validation Failed for: %s" % task.taskName)

    def SystemTestIndexer(self):
        self.loop = 1
        self.log_query_failures = False
        self.initial_setup()

        self.PrintStep("Crash indexer with Loading of docs")
        self.crash_indexer(num_kills=2, graceful=False)

        self.loop = 0
        while self.loop < self.iterations:
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
            self.rebl_services = self.input.param("rebl_services", "index").split("-")
            self.rebl_nodes = self.input.param("rebl_nodes", 1)
            # self.sleep(300)
            for service in self.rebl_services:
                self.PrintStep("Step 5: Rebalance in of {} node with Loading of docs".format(service))
    
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes, nodes_out=0,
                                                services=[service]*self.rebl_nodes)
    
                if self.stop_rebalance:
                    rebalance_task = self.pause_rebalance()
                else:
                    rebalance_task = self.abort_rebalance(rebalance_task, "kill_indexer", self.cluster.index_nodes)
    
                if rebalance_task is not None:
                    self.task_manager.get_task_result(rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")
                self.print_stats(self.cluster)
    
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
                self.PrintStep("Crash indexer with Loading of docs")
                self.crash_indexer(num_kills=2, graceful=False)

                self.PrintStep("Step 6: Rebalance Out of {} node with Loading of docs".format(service))
                rebalance_task = self.rebalance(nodes_in=0, nodes_out=self.rebl_nodes,
                                                services=[service]*self.rebl_nodes)
                if self.stop_rebalance:
                    rebalance_task = self.pause_rebalance()
                else:
                    rebalance_task = self.abort_rebalance(rebalance_task, "kill_indexer", self.cluster.index_nodes)
                if rebalance_task is not None:
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")

                self.PrintStep("Crash indexer with Loading of docs")
                self.crash_indexer(num_kills=2, graceful=False)
    
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
    
                self.PrintStep("Step 9: Rebalance In_Out of {} nodes with Loading of docs".format(service))
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes+1, nodes_out=self.rebl_nodes,
                                                services=[service]*(self.rebl_nodes+1))
                if self.stop_rebalance:
                    rebalance_task = self.pause_rebalance()
                else:
                    rebalance_task = self.abort_rebalance(rebalance_task, "kill_indexer", self.cluster.index_nodes)
                if rebalance_task is not None:
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")

                self.PrintStep("Crash indexer with Loading of docs")
                self.crash_indexer(num_kills=2, graceful=False)
    
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
    
                self.PrintStep("Step 10: Swap Rebalance of {} Nodes with Loading of docs".format(service))
    
                rebalance_task = self.rebalance(nodes_in=self.rebl_nodes, nodes_out=self.rebl_nodes,
                                                services=[service]*(self.rebl_nodes))
                if self.stop_rebalance:
                    rebalance_task = self.pause_rebalance()
                else:
                    rebalance_task = self.abort_rebalance(rebalance_task, "kill_indexer", self.cluster.index_nodes)
                if rebalance_task is not None:
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")

                self.PrintStep("Crash indexer with Loading of docs")
                self.crash_indexer(num_kills=2, graceful=False)
    
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
                self.PrintStep("Step 11: Failover %s node and RebalanceOut that node \
                with loading in parallel" % self.num_replicas)
                graceful = False
                failover_nodes = self.cluster.index_nodes
                nodes = [node for node in failover_nodes if node.ip != self.cluster.master.ip]
                self.rest = RestConnection(self.cluster.master)
                self.chosen = random.sample(nodes, self.num_replicas)
    
                # Mark Node for failover
                self.success_failed_over = True
                for node in self.chosen:
                    failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                    node.id = failover_node.id
                    success_failed_over = self.rest.fail_over(failover_node.id,
                                                                   graceful=graceful)
                    self.success_failed_over = self.success_failed_over and success_failed_over
                    self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                    self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
                self.sleep(600, "Waiting for data to go in after failover.")
    
                self.nodes = self.rest.node_statuses()
                self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                                    ejectedNodes=[node.id for node in self.chosen])
                self.assertTrue(self.rest.monitorRebalance(progress_count=500000), msg="Rebalance failed")
                servs_out = []
                for failed_over in self.chosen:
                    servs_out += [node for node in self.cluster.servers
                                  if node.ip == failed_over.ip]
                    self.cluster.index_nodes.remove(failed_over)
                self.available_servers += servs_out
                print("KV nodes in cluster: %s" % [server.ip for server in self.cluster.kv_nodes])
                print("CBAS nodes in cluster: %s" % [server.ip for server in self.cluster.cbas_nodes])
                print("INDEX nodes in cluster: %s" % [server.ip for server in self.cluster.index_nodes])
                print("FTS nodes in cluster: %s" % [server.ip for server in self.cluster.fts_nodes])
                print("QUERY nodes in cluster: %s" % [server.ip for server in self.cluster.query_nodes])
                print("EVENTING nodes in cluster: %s" % [server.ip for server in self.cluster.eventing_nodes])
                print("AVAILABLE nodes for cluster: %s" % [server.ip for server in self.available_servers])
                self.PrintStep("Crash indexer with Loading of docs")

                self.crash_indexer(num_kills=2, graceful=False)
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
                self.PrintStep("Step 13: Failover a node and FullRecovery\
                 that node")
                failover_nodes = self.cluster.index_nodes
                nodes = [node for node in failover_nodes if node.ip != self.cluster.master.ip]
                self.rest = RestConnection(self.cluster.master)
                self.chosen = random.sample(nodes, self.num_replicas)
    
                # Mark Node for failover
                self.success_failed_over = True
                for node in self.chosen:
                    failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                    node.id = failover_node.id
                    success_failed_over = self.rest.fail_over(failover_node.id,
                                                                   graceful=graceful)
                    self.success_failed_over = self.success_failed_over and success_failed_over
                    self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                    self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
                self.sleep(600, "Waiting for data to go in after failover.")
                self.rest.monitorRebalance(progress_count=50000)
    
                # Mark Node for full recovery
                if self.success_failed_over:
                    for node in self.chosen:
                        self.rest.set_recovery_type(otpNode=node.id,
                                                    recoveryType="full")
                self.sleep(60, "Waiting for full recovery to finish and settle down cluster.")
                rebalance_task = self.task.async_rebalance(
                    self.cluster, [], [],
                    retry_get_process_num=3000,
                    validate_bucket_ranking=False)
    
                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")

                self.PrintStep("Crash indexer with Loading of docs")
                self.crash_indexer(num_kills=2, graceful=False)
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
                nodes = [node for node in self.cluster.kv_nodes if node.ip != self.cluster.master.ip]
                self.rest = RestConnection(self.cluster.master)
                self.chosen = random.sample(nodes, self.num_replicas)
    
                # Mark Node for failover
                self.success_failed_over = True
                for node in self.chosen:
                    failover_node = self.cluster_util.find_node_info(self.cluster.master, node)
                    node.id = failover_node.id
                    success_failed_over = self.rest.fail_over(failover_node.id,
                                                                   graceful=True)
                    self.success_failed_over = self.success_failed_over and success_failed_over
                    self.sleep(60, "Waiting for failover to finish and settle down cluster.")
                    self.assertTrue(self.rest.monitorRebalance(progress_count=50000), msg="Failover -> Rebalance failed")
                self.sleep(600, "Waiting for data to go in after failover.")
                self.rest.monitorRebalance(progress_count=50000)
    
                # Mark Node for delta recovery
                if self.success_failed_over:
                    for node in self.chosen:
                        self.rest.set_recovery_type(otpNode=node.id,
                                                    recoveryType="delta")
    
                self.sleep(60, "Waiting for delta recovery to finish and settle down cluster.")
                rebalance_task = self.task.async_rebalance(
                    self.cluster, [], [],
                    retry_get_process_num=3000,
                    validate_bucket_ranking=False)
                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")

                self.PrintStep("Crash indexer with Loading of docs")
                self.crash_indexer(num_kills=2, graceful=False)
    
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
    
                self.loop += 1
        self.mutations = False
        self.mutation_th.join()

        self.log.info("Volume Test Run Complete")

    def normal_mutations(self):
        self.create_perc = 25
        self.update_perc = 25
        self.delete_perc = 25
        self.expiry_perc = 25
        self.read_perc = 25
        self.mutation_perc = self.input.param("mutation_perc", 100)
        self.doc_ops = self.input.param("doc_ops", "")
        pattern = None
        self.loader_tasks = list()
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(":")
            perc = 100/len(self.doc_ops)
            self.expiry_perc = perc if "expiry" in self.doc_ops else 0
            self.create_perc = perc if "create" in self.doc_ops else 0
            self.update_perc = perc if "update" in self.doc_ops else 0
            self.delete_perc = perc if "delete" in self.doc_ops else 0
            self.read_perc = perc if "read" in self.doc_ops else 0
            pattern = [self.create_perc, self.read_perc, self.update_perc, self.delete_perc, self.expiry_perc]
        while self.mutations:
            self.mutate += 1
            for bucket in self.cluster.buckets:
                bucket.loadDefn["pattern"] = pattern or bucket.loadDefn.get("pattern")
                bucket.loadDefn["load_type"] = self.doc_ops if self.doc_ops else bucket.loadDefn.get("load_type")
                self.generate_docs(bucket=bucket)
                bucket.original_ops = bucket.loadDefn["ops"]
                bucket.loadDefn["ops"] = self.input.param("rebl_ops_rate", 5000)
                pprint.pprint(bucket.loadDefn)
            self.loader_tasks = self.perform_load(wait_for_load=False, cluster=self.cluster)
            for task in self.loader_tasks:
                self.task_manager.get_task_result(task)
                self.loader_tasks.remove(task)
            self.check_index_pending_mutations(self.cluster)

    def sift_mutations(self):
        self.loader_tasks = list()
        while self.mutations:
            self.expiry_perc = 0
            self.create_perc = 0
            self.update_perc = 100
            self.delete_perc = 0
            self.read_perc = 0
            self.mutate += 1
            for bucket in self.cluster.buckets:
                bucket.loadDefn["ops"] = self.input.param("rebl_ops_rate", 10000)
                self.gtm = False
            self.loader_tasks = self.load_sift_data(
                cluster=self.cluster,
                buckets=self.cluster.buckets,
                validate_data=False,
                wait_for_stats=False,
                wait_for_load=False)
            for task in self.loader_tasks:
                self.task_manager.get_task_result(task)
                self.loader_tasks.remove(task)
            self.check_index_pending_mutations(self.cluster)

    def end_step_checks(self, msg=None):
        if msg:
            self.log.info("Checking for crashes/panics after {}".format(msg))
        self.print_stats(self.cluster)
        result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        self.assertFalse(
            result,
            "CRASH | CRITICAL | WARN messages found in cb_logs")
