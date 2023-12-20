'''
Created on 15-Apr-2021

@author: riteshagarwal
'''
import random
from threading import Thread

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from Cb_constants.CBServer import CbServer
from aGoodDoctor.opd import OPD
from basetestcase import BaseTestCase
from com.couchbase.test.sdk import Server
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from vectorFTS import DoctorFTS, FTSQueryLoad


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
        self.num_scopes = self.input.param("num_scopes", 1)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.kv_nodes = self.nodes_init
        self.fts_nodes = self.input.param("fts_nodes", 0)
        self.mutation_perc = 100
        self.doc_ops = self.input.param("doc_ops", "create")
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(':')

        self.threads_calculation()
        self.rest = RestConnection(self.servers[0])
        self.op_type = self.input.param("op_type", "create")
        self.dgm = self.input.param("dgm", None)
        self.available_servers = self.cluster.servers[self.nodes_init:]
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
        self.vector = self.input.param("vector", True)
        self.loader_dict = None
        self.parallel_reads = self.input.param("parallel_reads", False)
        self._data_validation = self.input.param("data_validation", True)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.key_type = self.input.param("key_type", "SimpleKey")
        self.val_type = self.input.param("val_type", "Hotel")
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.index_timeout = self.input.param("index_timeout", 86400)
        self.assert_crashes_on_load = self.input.param("assert_crashes_on_load",
                                                       True)
        self.gtm = self.input.param("gtm", False)
        #######################################################################
        self.capella_run = self.input.param("capella_run", False)
        self.sdk_client_pool = self.bucket_util.initialize_java_sdk_client_pool()

        self.PrintStep("Step 1: Create a %s node cluster" % self.nodes_init)
        if self.nodes_init > 1:
            services = list()
            nodes_init = self.cluster.servers[1:self.nodes_init]
            if self.services_init:
                for service in self.services_init.split("-"):
                    services.append(service.replace(":", ","))
            if not self.capella_run:
                self.task.rebalance(self.cluster, nodes_init, [], services=services*len(nodes_init))
                if self.nebula:
                    self.nebula_details[self.cluster].update_server_list()
        self.available_servers = self.cluster.servers[len(self.cluster.nodes_in_cluster):]

        self.default = {
            "valType": "Hotel",
            "scopes": 1,
            "collections": self.input.param("collections", 2),
            "num_items": self.input.param("num_items", 50000),
            "start": 0,
            "end": self.input.param("num_items", 50000),
            "ops": self.input.param("ops_rate", 2000),
            "doc_size": 1024,
            "pattern": [0, 80, 20, 0, 0], # CRUDE
            "load_type": ["read", "update"],
            "2iQPS": 0,
            "ftsQPS": 10,
            "cbasQPS": 0,
            "collections_defn": [
                {
                    "valType": "Hotel",
                    "2i": [0, 0],
                    "FTS": [self.input.param("collections", 2),
                            self.input.param("collections", 2)],
                    "cbas": [0, 0, 0]
                    },
                {
                    "valType": "Hotel",
                    "2i": [0, 0],
                    "FTS": [0, 0],
                    "cbas": [0, 0, 0]
                    }
                ]
            }
        self.num_collections = self.default["collections"]
        self.load_defn = list()
        self.load_defn.append(self.default)
        #######################################################################
        self.PrintStep("Step 2: Create required buckets and collections.")
        if self.num_buckets > 10:
            self.bucket_util.change_max_buckets(self.cluster.master,
                                                self.num_buckets)
        self.create_buckets()

        self.rest = RestConnection(self.cluster.master)
        self.af_timeout = self.input.param("af_timeout", 600)
        self.af_enable = self.input.param("af_enable", False)
        self.assertTrue(
            self.rest.update_autofailover_settings(self.af_enable,
                                                   self.af_timeout),
            "AutoFailover disabling failed")

        server = self.rest.get_nodes_self()
        if self.fts_nodes > 0:
            self.rest.set_service_mem_quota({CbServer.Settings.FTS_MEM_QUOTA:
                                             int(server.mcdMemoryReserved - 100
                                                 )})
            nodes = len(self.cluster.nodes_in_cluster)
            self.task.rebalance(self.cluster,
                                self.servers[nodes:nodes+self.fts_nodes], [],
                                services=["fts"]*self.fts_nodes)
            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in self.cluster.fts_nodes]

        if self.cluster.fts_nodes:
            self.drFTS = DoctorFTS(self.cluster, self.bucket_util)

        print self.available_servers
        self.writer_threads = self.input.param("writer_threads", "disk_io_optimized")
        self.reader_threads = self.input.param("reader_threads", "disk_io_optimized")
        self.storage_threads = self.input.param("storage_threads", 40)
        self.bucket_util.update_memcached_num_threads_settings(
            self.cluster.master,
            num_writer_threads=self.writer_threads,
            num_reader_threads=self.reader_threads,
            num_storage_threads=self.storage_threads)
        self.ftsQL = list()

    def create_buckets(self):
        self.log.info("Create CB buckets")
        # Create Buckets
        self.log.info("Get the available memory quota")
        rest = RestConnection(self.cluster.master)
        self.info = rest.get_nodes_self()
        # threshold_memory_vagrant = 100
        kv_memory = int(self.info.memoryQuota*0.8)
        ramQuota = self.input.param("ramQuota", kv_memory)
        buckets = ["default"] * self.num_buckets
        bucket_type = self.bucket_type.split(';') * self.num_buckets
        for i in range(self.num_buckets):
            bucket = Bucket(
                {Bucket.name: buckets[i] + str(i),
                 Bucket.ramQuotaMB: ramQuota / self.num_buckets,
                 Bucket.maxTTL: self.bucket_ttl,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.bucketType: bucket_type[i],
                 Bucket.durabilityMinLevel: self.bucket_durability_level,
                 Bucket.flushEnabled: self.flush_enabled,
                 Bucket.fragmentationPercentage: self.fragmentation})
            self.bucket_params = {
                "name": bucket.name,
                "bucketConflictResolution": "seqno",
                "memoryAllocationInMb": bucket.ramQuotaMB,
                "flush": bucket.flushEnabled,
                "replicas": bucket.replicaNumber,
                "storageBackend": bucket.storageBackend,
                "durabilityLevel": bucket.durabilityMinLevel,
                "timeToLive": {"unit": "seconds", "value": bucket.maxTTL}
            }
            bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
            self.bucket_util.create_bucket(self.cluster, bucket)
            self.bucket_util.get_updated_bucket_server_list(self.cluster, bucket)

        num_clients = self.input.param("clients_per_db",
                                       min(5, bucket.loadDefn.get("collections")))
        for bucket in self.cluster.buckets:
            self.create_sdk_client_pool([bucket],
                                        num_clients)
        self.create_required_collections(self.cluster)

    def create_sdk_client_pool(self, buckets, req_clients_per_bucket):
        for bucket in buckets:
            self.log.info("Using SDK endpoint %s" % self.cluster.master.ip)
            server = Server(self.cluster.master.ip, self.cluster.master.port,
                            self.cluster.master.rest_username,
                            self.cluster.master.rest_password,
                            str(self.cluster.master.memcached_port))
            self.sdk_client_pool.create_clients(
                bucket.name, server, req_clients_per_bucket)
            bucket.clients = self.sdk_client_pool.clients.get(bucket.name).get("idle_clients")
        self.sleep(1, "Wait for SDK client pool to warmup")

    def create_required_collections(self, cluster, buckets=None):
        buckets = buckets or cluster.buckets
        self.scope_name = self.input.param("scope_name", "_default")

        def create_collections(bucket):
            node = cluster.master
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                if bucket.loadDefn.get("collections") > 0:
                    self.collection_prefix = self.input.param("collection_prefix",
                                                              "VolumeCollection")

                    for i in range(bucket.loadDefn.get("collections")):
                        collection_name = self.collection_prefix + str(i)
                        self.bucket_util.create_collection(node,
                                                           bucket,
                                                           scope,
                                                           {"name": collection_name})
                        self.sleep(0.1)

                collections = bucket.scopes[scope].collections.keys()
                self.log.debug("Collections list == {}".format(collections))

        for bucket in buckets:
            if bucket.loadDefn.get("scopes") > 1:
                self.scope_prefix = self.input.param("scope_prefix",
                                                     "VolumeScope")
                node = cluster.master or bucket.nebula_endpoint
                for i in range(bucket.loadDefn.get("scopes")):
                    scope_name = self.scope_prefix + str(i)
                    self.log.info("Creating scope: %s"
                                  % (scope_name))
                    self.bucket_util.create_scope(node,
                                                  bucket,
                                                  {"name": scope_name})
                    self.sleep(0.1)
        threads = []
        for bucket in buckets:
            th = Thread(
                    target=create_collections,
                    name="{}_create_collection".format(bucket.name),
                    args=(bucket,))
            threads.append(th)
            th.start()
        for thread in threads:
            thread.join()

    def tearDown(self):
        self.check_dump_thread = False
        self.stop_crash = True
        BaseTestCase.tearDown(self)

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
            for bucket in self.cluster.buckets:
                self.generate_docs(doc_ops=["create"],
                                   create_start=0,
                                   create_end=bucket.loadDefn.get("num_items")/2,)
            self.perform_load(validate_data=False)

            self.PrintStep("Step 3: Create %s items sequentially" % self.num_items)
            for bucket in self.cluster.buckets:
                self.generate_docs(doc_ops=["create"],
                                   create_start=bucket.loadDefn.get("num_items")/2,
                                   create_end=bucket.loadDefn.get("num_items"))
            self.perform_load(validate_data=False)
            self.ops_rate = self.input.param("rebl_ops_rate", self.ops_rate)
            ###################################################################
            if self.loop == 0:
                if self.fts_nodes:
                    self.drFTS.create_fts_indexes(self.cluster.buckets)
                    status = self.drFTS.wait_for_fts_index_online(self.cluster.buckets,
                                                                  self.index_timeout)
                    self.assertTrue(status, "FTS index build failed.")
                    for bucket in self.cluster.buckets:
                        if bucket.loadDefn.get("ftsQPS", 0) > 0:
                            ql = FTSQueryLoad(bucket, self.cluster)
                            ql.start_query_load()
                            self.ftsQL.append(ql)

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
            self.key_type = "CircularKey"
            self.create_perc = 25
            self.update_perc = 25
            self.delete_perc = 25
            self.expiry_perc = 25
            self.read_perc = 25
            self.mutation_perc = self.input.param("mutation_perc", 100)
            self.generate_docs(doc_ops=["update", "delete", "read", "create"])
            tasks = self.perform_load(wait_for_load=False)
            self.rebl_services = self.input.param("rebl_services", ["fts"])
            self.rebl_nodes = self.input.param("rebl_nodes", 1)

            self.PrintStep("Step 5: Rebalance in of KV node with Loading of docs")

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

            self.PrintStep("Step 6: Rebalance Out of KV node with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=self.rebl_nodes,
                                            services=self.rebl_services*self.rebl_nodes)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            #end_step_checks()

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

            self.PrintStep("Step 9: Rebalance In_Out of KV nodes with Loading of docs")
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

            self.PrintStep("Step 10: Swap Rebalance of KV Nodes with Loading of docs")

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
            self.PrintStep("Step 11: Failover %s node and RebalanceOut that node \
            with loading in parallel" % self.num_replicas)
            nodes = [node for node in self.cluster.kv_nodes if node.ip != self.cluster.master.ip]
            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            std = self.std_vbucket_dist or 1.0

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
                self.cluster.kv_nodes.remove(failed_over)
            self.available_servers += servs_out
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
                nodes,
                self.cluster.buckets)

            self.bucket_util.data_analysis_active_replica_all(
                disk_active_dataset, disk_replica_dataset,
                self.cluster.servers[:self.nodes_in + self.nodes_init],
                self.cluster.buckets, path=None)
            nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
            self.bucket_util.vb_distribution_analysis(
                self.cluster,
                servers=self.cluster.kv_nodes, buckets=self.cluster.buckets,
                num_replicas=self.num_replicas,
                std=std, total_vbuckets=self.cluster.vbuckets)

            ###################################################################
            extra_node_gone = self.num_replicas - 1
            if extra_node_gone > 0:

                self.PrintStep("Step 12: Rebalance in of KV Node with Loading of docs")

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
            self.PrintStep("Step 13: Failover a node and FullRecovery\
             that node")
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
                self.cluster, [], [],
                retry_get_process_num=3000)

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

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
                self.cluster, [], [],
                retry_get_process_num=3000)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")
            end_step_checks()

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
                if len(self.cluster.kv_nodes) > self.nodes_init:
                    rebalance_task = self.rebalance(nodes_in=[], nodes_out=int(len(self.cluster.kv_nodes) + 1 - self.nodes_init),
                                                    services=["kv"])
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.assertTrue(rebalance_task.result, "Rebalance Failed")
            else:
                self.log.info("Volume Test Run Complete")
            self.init_doc_params()
