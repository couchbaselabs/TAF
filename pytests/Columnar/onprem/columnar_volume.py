"""
Created on 30-May-2025

@author: ritesh.agarwal@couchbase.com
"""

from collections import defaultdict
import json
import random
import string
import threading
import time
from CbasLib.CBASOperations import CBASHelper
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from cb_constants import CbServer
from bucket_utils.bucket_ready_functions import CollectionUtils, JavaDocLoaderUtils
from cb_server_rest_util.rest_client import RestConnection
from sdk_client3 import SDKClient

from .columnar_onprem_base import ColumnarOnPremBase
from couchbase_utils.security_utils.x509main import x509main
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil
from pytests.aGoodDoctor.opd import OPD
from pytests.aGoodDoctor.goldfish.CbasUtil import DoctorCBAS, CBASQueryLoad
from pytests.aGoodDoctor.goldfish.datasources import CouchbaseRemoteCluster, MongoDB, KafkaClusterUtils, MongoDocLoading
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil
from TestInput import TestInputSingleton

_input = TestInputSingleton

class ColumnarOnPremVolumeTest(ColumnarOnPremBase, OPD):
    """
    This test is meant to validate columnar server build before promoting
    it to AMI for Capella Columnar
    """
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
        super(ColumnarOnPremVolumeTest, self).setUp()
        self.init_doc_params()
        self.threads_calculation()
        self.num_collections = self.input.param("num_collections", 1)
        self.xdcr_collections = self.input.param("xdcr_collections", self.num_collections)
        self.num_collections_bkrs = self.input.param("num_collections_bkrs", self.num_collections)
        self.num_scopes = self.input.param("num_scopes", 1)
        self.xdcr_scopes = self.input.param("xdcr_scopes", self.num_scopes)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.skip_read_on_error = False
        self.suppress_error_table = False
        self.track_failures = self.input.param("track_failures", True)
        self.loader_dict = None
        self._data_validation = self.input.param("data_validation", True)
        self.turn_cluster_off = self.input.param("cluster_off", False)
        self.key_type = self.input.param("key_type", "SimpleKey")
        self.val_type = self.input.param("val_type", "SimpleValue")
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.gtm = self.input.param("gtm", False)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.index_timeout = self.input.param("index_timeout", 3600)
        self.load_defn = list()
        self.stop_run = False
        self.skip_init = self.input.param("skip_init", False)
        self.query_result = True
        self.default_workload = {
            "valType": "Hotel",
            "database": 1,
            "collections": self.input.param("collections", 1),
            "scopes": 1,
            "num_items": self.input.param("num_items", 1000000),
            "start": 0,
            "end": self.input.param("num_items", 1000000),
            "ops": self.input.param("ops_rate", 20000),
            "doc_size": 1024,
            "pattern": [0, 0, 100, 0, 0], # CRUDE
            "load_type": ["update"],
            "cbasQPS": self.input.param("cbasQPS", 10),
            "cbas": [self.input.param("cbas_collections", 2), 5]
            }
        self.load_defn.append(self.default_workload)

        self.data_sources = defaultdict(list)
        self.mutation_perc = self.input.param("mutation_perc", 100)
        self.ql = list()
        self.ftsQL = list()
        self.cbasQL = list()
        self.drCBAS = DoctorCBAS()
        self.query_cancel_ths = list()
        self.bucket_history_retention_bytes = self.input.param("bucket_history_retention_bytes", 0)
        self.bucket_history_retention_seconds = self.input.param("bucket_history_retention_seconds", 0)
        self.steady_state_workload_sleep = self.input.param("steady_state_workload_sleep", 600)
        JavaDocLoaderUtils(self.bucket_util, self.cluster_util)

    def setupRemoteCouchbase(self):
        # Updating Remote Links Spec
        status, certificate, header = x509main(
            self.remote_cluster.master)._get_cluster_ca_cert()
        if status:
            certificate = json.loads(certificate)["cert"]["pem"]
            self.remote_cluster.root_ca = certificate
        remoteCluster = CouchbaseRemoteCluster(self.remote_cluster, self.bucket_util)
        remoteCluster.loadDefn = self.default_workload
        self.data_sources["remoteCouchbase"].append(remoteCluster)
        if not self.skip_init:
            self.create_required_buckets(self.remote_cluster)
        else:
            for i, bucket in enumerate(self.remote_cluster.buckets):
                bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                num_clients = self.input.param("clients_per_db",
                                               min(5, bucket.loadDefn.get("collections")))
                SiriusCouchbaseLoader.create_clients_in_pool(
                    self.remote_cluster.master, self.remote_cluster.master.rest_username,
                    self.remote_cluster.master.rest_password,
                    bucket.name, req_clients=num_clients)
                self.create_sdk_client_pool(self.remote_cluster, self.remote_cluster.buckets, 1)
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

    def setupMongo(self, atlas=False):
        if not atlas:
            mongo_hostname = self.input.datasources.get("onprem_mongo")
            mongo_username = self.input.datasources.get("onprem_mongo_user")
            mongo_password = self.input.datasources.get("onprem_mongo_pwd")
            mongo_atlas = False
        else:
            mongo_hostname = self.input.datasources.get("atlas_mongo")
            mongo_username = self.input.datasources.get("atlas_mongo_user")
            mongo_password = self.input.datasources.get("atlas_mongo_pwd")
            mongo_atlas = True

        self.mongo_db = self.input.param("mongo_db", None)
        mongo = MongoDB(mongo_hostname, mongo_username,
                        mongo_password, self.mongo_db, mongo_atlas)
        mongo.loadDefn = self.default_workload
        mongo.set_collections()
        mongo.key = "test_docs-"
        mongo.key_size = 18
        mongo.key_type = "Circular"
        
        mongo.setup_kafka_connectors("taf_volume")
        self.data_sources["mongo"].append(mongo)

    def load_mongo_cluster(self):
        self.mongo_doc_loading = MongoDocLoading(self.doc_loading_tm, "test_docs-", 18, 256, "Circular", "SimpleValue",
                                            1, "mongo_load", 10000, False, True, 0)
        for mongo in self.data_sources["mongo"]:
            self.generate_docs(doc_ops=["create"],
                               create_start=0,
                               create_end=mongo.loadDefn.get("num_items"),
                               bucket=mongo)
            if not self.skip_init:
                self.mongo_doc_loading.perform_load(self.data_sources["mongo"], wait_for_load=True,
                                               overRidePattern=[100, 0, 0, 0, 0])

    def check_kafka_topics(self, mongo):
        start_time = time.time()
        self.kafka_util = KafkaClusterUtils()
        while time.time() < start_time + 3600:
            topics = self.kafka_util.listKafkaTopics(prefix=mongo.prefix + "." + mongo.source_name)
            if topics and len(topics) == len(mongo.collections):
                self.log.info("Kafka topics created: %s" % topics)
                self.log.info("Kafka topics are created. Good to go!!")
                break
            else:
                self.log.info("Kafka topics aren't created. waiting...")
                self.log.debug("Current Topics: %s" % self.kafka_util.listKafkaTopics(
                    prefix=mongo.prefix + "." + mongo.source_name))
                time.sleep(10)

    def teardownMongo(self):
        for dataSource in self.data_sources["mongo"]:
            dataSource.drop()

    def tearDownKafka(self):
        self.kafka_util = KafkaClusterUtils()
        for dataSource in self.data_sources["mongo"]:
            self.kafka_util.deleteKafkaTopics(dataSource.kafka_topics)

    def setup_columnar_sdk_clients(self, columnar):
        from datetime import timedelta
        import couchbase_columnar
        from couchbase_columnar.cluster import Cluster
        from couchbase_columnar.credential import Credential
        from couchbase_columnar.options import (ClusterOptions,SecurityOptions,TimeoutOptions)
        from couchbase_columnar.common.core._certificates import _Certificates
        sec_opts = SecurityOptions(disable_server_certificate_verification=True)
        timeout_ops = TimeoutOptions(connect_timeout=timedelta(seconds=60), dispatch_timeout=timedelta(seconds=30))
        opts = ClusterOptions(security_options=sec_opts, timeout_options=timeout_ops, dump_configuration=True)
        connstr = 'couchbases://' + columnar.master.ip
        cred = Credential.from_username_and_password(columnar.master.rest_username, columnar.master.rest_password)
        cluster = Cluster.create_instance(connstr, cred, opts)
        columnar.SDKClients = [cluster]
        self.sleep(1, "Wait for SDK client pool to warmup")

    def load_remote_couchbase_clusters(self):
        if not self.skip_init:
            if self.val_type == "siftBigANN":
                JavaDocLoaderUtils.load_sift_data(self.cluster, self.cluster.buckets,
                                                  overRidePattern={"create": 100, "update": 0, "delete": 0, "read": 0, "expiry": 0},
                                                  wait_for_stats=False,
                                                  validate_data=False)
            else:
                JavaDocLoaderUtils.load_data(self.cluster, self.cluster.buckets,
                                             overRidePattern={"create": 100, "update": 0, "delete": 0, "read": 0, "expiry": 0})

    def tearDown(self):
        self.stop_run = True
        for ql in self.cbasQL:
            ql.stop_query_load()
        
        # Cleanup MongoDB and Kafka resources
        if self.data_sources["mongo"]:
            for mongo in self.data_sources["mongo"]:
                mongo.client.close()
            self.teardownMongo()
            self.tearDownKafka()
            
        if not self.skip_teardown_cleanup:
            self.columnar_cbas_utils.cleanup_cbas(self.analytics_cluster)
        super(ColumnarOnPremVolumeTest, self).tearDown()

    def update_cluster_state(self, cluster):
        status, content, _ = CBASHelper(cluster.master).get_cluster_details()
        if status:
            cc_ip = json.loads(content)["ccNodeName"].split(":")[0]
            cluster.cbas_cc_node = [server for server in cluster.servers if server.ip == cc_ip][0]
            cluster.master = cluster.cbas_cc_node
            state = json.loads(content)["state"]
            cluster.state = state

    def cluster_state_monitor(self, cluster):
        while not self.stop_run:
            try:
                status, content, _ = CBASHelper(cluster.master).get_cluster_details()
                if status:
                    cc_ip = json.loads(content)["ccNodeName"].split(":")[0]
                    cluster.cbas_cc_node = [server for server in cluster.servers if server.ip == cc_ip][0]
                    cluster.master = cluster.cbas_cc_node
                    state = json.loads(content)["state"]
                    if state != "ACTIVE" or cluster.state != "ACTIVE":
                        self.log.critical("Columnar cluster state is {}".format(state))
                    cluster.state = state
            except Exception as e:
                print(e)
                import traceback
                traceback.print_exc()
            self.sleep(30)


    def infra_setup(self):
        self.monitor_query_status()
        
        # Setup MongoDB if enabled
        if self.input.param("onPremMongo", False):
            self.setupMongo(atlas=False)
        if self.input.param("onCloudMongo", False):
            self.setupMongo(atlas=True)
        
        if self.input.param("remoteCouchbase", True):
            self.setupRemoteCouchbase()
        
        # Load MongoDB data and check Kafka topics
        if self.data_sources["mongo"]:
            self.load_mongo_cluster()
            for dataSource in self.data_sources["mongo"]:
                self.check_kafka_topics(dataSource)
        
        state_monitor = threading.Thread(target=self.cluster_state_monitor,
                                               kwargs={"cluster": self.analytics_cluster})
        state_monitor.start()

        self.setup_columnar_sdk_clients(self.analytics_cluster)
        for datasources in self.data_sources.values():
            self.drCBAS.create_links(self.analytics_cluster, datasources)

        self.load_remote_couchbase_clusters()
        
        for key in self.data_sources.keys():
            if key == "s3":
                continue
            result = self.drCBAS.wait_for_ingestion(
                self.analytics_cluster, self.data_sources[key], self.index_timeout)
            self.assertTrue(result, "CBAS ingestion couldn't complete in time: %s" % self.index_timeout)

        for data_sources in self.data_sources.values():
            for data_source in data_sources:
                if data_source.loadDefn.get("cbasQPS", 0) > 0:
                    ql = CBASQueryLoad(self.analytics_cluster, data_source)
                    ql.start_query_load()
                    self.cbasQL.append(ql)

        if self.input.param("cancel_queries", False):
            self.cancel_queries_thread()

    def cancel_active_requests(self):
        while not self.stop_run:
            requests = self.cbas_util.get_all_active_requests(self.analytics_cluster)
            for request in requests:
                if request['state'] == "running" and \
                    "select" in request['statement'].lower() and \
                        "active_requests" not in request['statement'].lower() and \
                            request['clientContextID'] not in ["", "null", "None", None]:
                    self.cbas_util.delete_request(self.analytics_cluster, request['clientContextID'])
                    self.log.info("Cancelled request: %s" % request['statement'])
            self.sleep(random.randint(1, 10))

    def cancel_queries_thread(self):
        th = threading.Thread(target=self.cancel_active_requests)
        th.start()
        self.query_cancel_ths.append(th)
        
    def live_kv_workload(self):
        self.log.info("Creating live KV workload")
        while not self.stop_run:
            self.tasks = list()
            for bucket in self.remote_cluster.buckets:
                bucket.loadDefn["ops"] = self.input.param("rebl_ops_rate", 10000)
                JavaDocLoaderUtils.generate_docs(bucket=bucket)
                JavaDocLoaderUtils.perform_load(cluster=self.remote_cluster, wait_for_load=True, validate_data=False)
                result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=False)
                if result:
                    self.log.critical("Core dump(s) found on analytics cluster node(s) after KV workload")
            self.sleep(10)

    def live_mongo_workload(self):
        self.log.info("Creating live MongoDB workload")
        while not self.stop_run:
            for dataSource in self.data_sources["mongo"]:
                self.mongo_doc_loading.mutate += 1
                JavaDocLoaderUtils.generate_docs(bucket=dataSource)
                self.mongo_doc_loading.perform_load(
                    [dataSource], wait_for_load=True)
            self.sleep(10)

    def test_columnar_volume(self):
        self.update_cluster_state(self.analytics_cluster)
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            self.input.param("vbucket_check", True), self.cbas_util)
        self.log.info("Creating Buckets, Scopes and Collection on Remote "
                        "cluster.")
        self.infra_setup()

        result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=True)
        if result:
            self.fail("Core dump(s) found on analytics cluster node(s) after infra setup")

        
        # Start KV workload thread
        kv_workload_thread = threading.Thread(target=self.live_kv_workload)
        kv_workload_thread.start()
        
        # Start MongoDB workload thread if MongoDB is enabled
        mongo_workload_thread = None
        if self.data_sources["mongo"]:
            mongo_workload_thread = threading.Thread(target=self.live_mongo_workload)
            mongo_workload_thread.start()
            
        self.sleep(10)

        # Create new collections
        loop = self.input.param("loop", 10)
        while loop > 0:
            self.log.info("Strating test iteration: {}".format(loop))
            self.ingestion_ths = list()
            for dataSource in self.data_sources["remoteCouchbase"] + self.data_sources["mongo"]:
                max_retries = 3
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        self.drCBAS.disconnect_link(self.analytics_cluster, dataSource.link_name)
                        break
                    except Exception as e:
                        retry_count += 1
                        self.log.warning(f"Failed to disconnect link {dataSource.link_name} on attempt {retry_count}: {str(e)}")
                        if retry_count < max_retries:
                            self.sleep(10, f"Retrying link disconnect in 10 seconds (attempt {retry_count + 1}/{max_retries})")
                        else:
                            self.log.error(f"Failed to disconnect link {dataSource.link_name} after {max_retries} attempts")
            self.sleep(60, "wait after previous link disconnect")
            dataSource.link_name = "{}_".format(dataSource.type) + ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(5)])
            dataSource.links.append(dataSource.link_name)
            dataSource.loadDefn.get("cbas")[1] = dataSource.loadDefn.get("cbas")[1] + self.default_workload.get("cbas")[1]
            self.drCBAS.create_links(self.analytics_cluster, [dataSource])
            th = threading.Thread(
                target=self.drCBAS.wait_for_ingestion,
                args=(self.analytics_cluster, [dataSource], self.index_timeout))
            th.start()
            self.ingestion_ths.append(th)

            self.PrintStep("Step 1: Rebalance-In a KV+CBAS node in analytics cluster")
            rebalance_task, self.analytics_cluster.available_servers = \
                self.rebalance_util.rebalance(
                cluster=self.analytics_cluster, cbas_nodes_in=1,
                available_servers=self.analytics_cluster.available_servers,
                in_node_services="kv,cbas",
                wait_for_complete=True, monitor_health=True)
            if not rebalance_task.result:
                self.fail("Error while Rebalance-In KV+CBAS node in analytics "
                        "cluster")
            self.log.info("Rebalance-In KV+CBAS node in analytics cluster completed")
            result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=True)
            if result:
                self.fail("Core dump(s) found on analytics cluster node(s) after rebalance-in")
            self.setup_columnar_sdk_clients(self.analytics_cluster)
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after rebalance in for {} seconds".format(self.steady_state_workload_sleep))

            self.PrintStep("Step 2: Rebalance-Out a KV+CBAS node in analytics cluster")
            rebalance_task, self.analytics_cluster.available_servers = \
                self.rebalance_util.rebalance(
                cluster=self.analytics_cluster, cbas_nodes_out=1,
                available_servers=self.analytics_cluster.available_servers,
                wait_for_complete=True, monitor_health=True)
            if not rebalance_task.result:
                self.fail("Error while Rebalance-Out KV+CBAS node in analytics "
                        "cluster")
            result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=True)
            if result:
                self.fail("Core dump(s) found on analytics cluster node(s) after rebalance-out")
            self.setup_columnar_sdk_clients(self.analytics_cluster)
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after rebalance out for {} seconds".format(self.steady_state_workload_sleep))
            self.analytics_cluster.rest = RestConnection(self.analytics_cluster.cbas_cc_node)

            self.PrintStep("Step 3: Rebalance-swap a KV+CBAS node in analytics cluster")
            rebalance_task, self.analytics_cluster.available_servers = \
                self.rebalance_util.rebalance(
                cluster=self.analytics_cluster, cbas_nodes_in=1, cbas_nodes_out=1,
                available_servers=self.analytics_cluster.available_servers,
                in_node_services="kv,cbas",
                wait_for_complete=True, monitor_health=True)
            if not rebalance_task.result:
                self.fail("Error while Rebalance-Swap KV+CBAS node in analytics "
                        "cluster")
            self.analytics_cluster.rest = RestConnection(self.analytics_cluster.cbas_cc_node)
            result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=True)
            if result:
                self.log.critical("Core dump(s) found on analytics cluster node(s) after rebalance-swap")
            self.setup_columnar_sdk_clients(self.analytics_cluster)
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after rebalance swap for {} seconds".format(self.steady_state_workload_sleep))

            self.PrintStep("Step 4: HardFailover+DeltaRecovery+RebalanceIn a KV+CBAS node in analytics cluster")
            self.analytics_cluster.available_servers, _, _ = \
                self.rebalance_util.failover(
                cluster=self.analytics_cluster, cbas_nodes=1,
                action="DeltaRecovery", available_servers=self.analytics_cluster.available_servers,
                failover_type="Hard", monitor_health=True)
            result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=True)
            if result:
                self.log.critical("Core dump(s) found on analytics cluster node(s) after hard failover and delta recovery")
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after hard failover and delta recovery for {} seconds".format(self.steady_state_workload_sleep))
                
            self.PrintStep("Step 5: HardFailover+FullRecovery+RebalanceIn a KV+CBAS node in analytics cluster")
            self.analytics_cluster.available_servers, _, _ = \
                self.rebalance_util.failover(
                cluster=self.analytics_cluster, cbas_nodes=1,
                action="FullRecovery", available_servers=self.analytics_cluster.available_servers,
                failover_type="Hard", monitor_health=True)
            result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=True)
            if result:
                self.log.critical("Core dump(s) found on analytics cluster node(s) after hard failover and full recovery")
            self.setup_columnar_sdk_clients(self.analytics_cluster)
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after hard failover and full recovery for {} seconds".format(self.steady_state_workload_sleep))
                
            self.PrintStep("Step 6: HardFailover+RebalanceOut a KV+CBAS node in analytics cluster")
            self.analytics_cluster.available_servers, _, _ = \
                self.rebalance_util.failover(
                cluster=self.analytics_cluster, cbas_nodes=1,
                action="RebalanceOut", available_servers=self.analytics_cluster.available_servers,
                failover_type="Hard", monitor_health=True)
            self.analytics_cluster.rest = RestConnection(self.analytics_cluster.cbas_cc_node)
            result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=True)
            if result:
                self.log.critical("Core dump(s) found on analytics cluster node(s) after hard failover and rebalance out")
            self.setup_columnar_sdk_clients(self.analytics_cluster)
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after hard failover and rebalance out for {} seconds".format(self.steady_state_workload_sleep))
                
            self.PrintStep("Step 7: Rebalance-In a KV+CBAS node in analytics cluster")
            rebalance_task, self.analytics_cluster.available_servers = \
                self.rebalance_util.rebalance(
                cluster=self.analytics_cluster, cbas_nodes_in=1,
                available_servers=self.analytics_cluster.available_servers,
                in_node_services="kv,cbas",
                wait_for_complete=True, monitor_health=True)
            if not rebalance_task.result:
                self.fail("Error while Rebalance-In KV+CBAS node in analytics "
                        "cluster")
            result = self.check_coredump_exist(self.analytics_cluster.nodes_in_cluster, force_collect=True)
            if result:
                self.log.critical("Core dump(s) found on analytics cluster node(s) after rebalance-in")
            self.setup_columnar_sdk_clients(self.analytics_cluster)
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after rebalance in for {} seconds".format(self.steady_state_workload_sleep))
                
            # self.PrintStep("Step 8: GracefulFailover+DeltaRecovery+RebalanceIn a KV+CBAS node in analytics cluster")
            # self.analytics_cluster.available_servers, _, _ = \
            #     self.rebalance_util.failover(
            #     cluster=self.analytics_cluster, cbas_nodes=1,
            #     action="DeltaRecovery", available_servers=self.analytics_cluster.available_servers,
            #     failover_type="Graceful")
                
            # self.PrintStep("Step 9: GracefulFailover+FullRecovery+RebalanceIn a KV+CBAS node in analytics cluster")
            # self.analytics_cluster.available_servers, _, _ = \
            #     self.rebalance_util.failover(
            #     cluster=self.analytics_cluster, cbas_nodes=1,
            #     action="FullRecovery", available_servers=self.analytics_cluster.available_servers,
            #     failover_type="Graceful")
                
            # self.PrintStep("Step 10: GracefulFailover+RebalanceOut a KV+CBAS node in analytics cluster")
            # self.analytics_cluster.available_servers, _, _ = \
            #     self.rebalance_util.failover(
            #     cluster=self.analytics_cluster, cbas_nodes=1,
            #     action="RebalanceOut", available_servers=self.analytics_cluster.available_servers,
            #     failover_type="Graceful")
                
            # self.analytics_cluster.rest = RestConnection(self.analytics_cluster.cbas_cc_node)

            # self.PrintStep("Step 11: Rebalance-In a KV+CBAS node in analytics cluster")
            # rebalance_task, self.analytics_cluster.available_servers = \
            #     self.rebalance_util.rebalance(
            #     cluster=self.analytics_cluster, cbas_nodes_in=1,
            #     available_servers=self.analytics_cluster.available_servers,
            #     in_node_services="kv,cbas")
            # if not self.rebalance_util.wait_for_rebalance_task_to_complete(
            #         rebalance_task, self.analytics_cluster, True, True):
            #     self.fail("Error while Rebalance-In KV+CBAS node in analytics "
            #               "cluster")
            for th in self.ingestion_ths:
                th.join()
            for th in self.query_cancel_ths:
                th.join()
            loop -= 1
        self.stop_run = True
        kv_workload_thread.join()
        if mongo_workload_thread:
            mongo_workload_thread.join()
