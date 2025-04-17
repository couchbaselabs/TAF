'''
Created on May 2, 2022

@author: ritesh.agarwal
'''
from _collections import defaultdict
import threading
import time

from CbasUtil import DoctorCBAS, CBASQueryLoad
from Jython_tasks.task import ScaleColumnarInstance
from aGoodDoctor.hostedOPD import hostedOPD
from basetestcase import BaseTestCase
from capella_utils.dedicated import CapellaUtils
from com.couchbase.test.sdk import SDKClient
from com.couchbase.test.sdk import Server
from datasources import KafkaClusterUtils
from datasources import MongoDB
from datasources import s3, CouchbaseRemoteCluster
from table_view import TableView
import random
import string
import json
from CbasLib.CBASOperations_Rest import CBASHelper


class Columnar(BaseTestCase, hostedOPD):

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
        hostedOPD.__init__(self)
        self.init_doc_params()
        self.threads_calculation()
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
        self.cbasQL = list()
        self.stop_run = False
        self.skip_init = self.input.param("skip_init", False)
        self.query_result = True
        self.default_workload = {
            "valType": "Hotel",
            "database": 1,
            "collections": self.input.param("collections", 1),
            "scopes": 1,
            "num_items": self.input.param("num_items", 10000),
            "start": 0,
            "end": self.input.param("num_items", 10000),
            "ops": self.input.param("ops_rate", 10000),
            "doc_size": 1024,
            "pattern": [0, 0, 100, 0, 0], # CRUDE
            "load_type": ["update"],
            "cbasQPS": self.input.param("cbasQPS", 1),
            "cbas": [self.input.param("cbas_collections", 10), 10]
            }
        self.load_defn.append(self.default_workload)

        self.data_sources = defaultdict(list)
        self.mutation_perc = self.input.param("mutation_perc", 100)
        self.ql = list()
        self.ftsQL = list()
        self.cbasQL = list()
        self.drCBAS = DoctorCBAS()
        self.mongo_workload_tasks = list()

    def setupRemoteCouchbase(self):
        for tenant in self.tenants:
            for provisionedcluster in tenant.clusters:
                resp = CapellaUtils.get_root_ca(self.pod, tenant, provisionedcluster.id)
                for cert in resp:
                    if "Couchbase Server" not in cert["subject"]:
                        provisionedcluster.root_ca = cert["pem"]
                        break
                remoteCluster = CouchbaseRemoteCluster(provisionedcluster, self.bucket_util)
                remoteCluster.loadDefn = self.default_workload
                self.data_sources["remoteCouchbase"].append(remoteCluster)
        self.setup_buckets()

    def setups3(self):
        s3_obj = s3(self.input.datasources.get("s3_access"),
                    self.input.datasources.get("s3_secret"))
        s3_obj.loadDefn = self.default_workload
        s3_obj.set_collections()
        self.data_sources["s3"].append(s3_obj)


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
        for mongo in self.data_sources["mongo"]:
            self.generate_docs(doc_ops=["create"],
                               create_start=0,
                               create_end=mongo.loadDefn.get("num_items"),
                               bucket=mongo)
            if not self.skip_init:
                mongo.perform_load(self.data_sources["mongo"], wait_for_load=True,
                                   overRidePattern=[100, 0, 0, 0, 0],
                                   tm=self.doc_loading_tm)

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
        columnar.SDKClients = list()
        master = Server(columnar.srv, columnar.master.port,
                        columnar.db_users[0].username, columnar.db_users[0].password,
                        str(columnar.master.memcached_port))
        client = SDKClient(master, "None")
        client.connectCluster()
        columnar.SDKClients.append(client)

    def load_remote_couchbase_clusters(self):
        self.initial_load()

    def tearDown(self):
        self.doc_loading_tm.abortAllTasks()
        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                for data_source in self.data_sources["mongo"] + self.data_sources["remoteCouchbase"]:
                    self.drCBAS.disconnect_link(cluster, data_source.link_name)
        # for tenant in self.tenants:
        #     for cluster in tenant.columnar_instances:
        #         for data_source in self.data_sources["mongo"]:
        #             self.drCBAS.wait_for_link_disconnect(cluster, data_source.link_name, 3600)

        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                self.drCBAS.drop_collections(cluster, self.data_sources["mongo"] + self.data_sources["remoteCouchbase"])
                self.drCBAS.drop_links(cluster, self.data_sources["mongo"] + self.data_sources["remoteCouchbase"])

        self.tearDownKafka()
        self.teardownMongo()
        self.check_dump_thread = False
        self.stop_crash = True
        self.stop_run = True
        for ql in self.cbasQL:
            ql.stop_query_load()
        self.sleep(10)
        BaseTestCase.tearDown(self)

    def infra_setup(self):
        self.monitor_query_status()
        if self.input.param("s3", False):
            self.setups3()

        if self.input.param("onPremMongo", False):
            self.setupMongo(atlas=False)
        if self.input.param("onCloudMongo", False):
            self.setupMongo(atlas=True)

        if self.input.param("remoteCouchbase", False):
            self.setupRemoteCouchbase()

        self.load_mongo_cluster()
        for dataSource in self.data_sources["mongo"]:
            self.check_kafka_topics(dataSource)

        for tenant in self.tenants:
            for columnar in tenant.columnar_instances:
                self.setup_columnar_sdk_clients(columnar)
                for datasources in self.data_sources.values():
                    self.drCBAS.create_links(columnar, datasources)

        self.load_remote_couchbase_clusters()

        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                for key in self.data_sources.keys():
                    if key == "s3":
                        continue
                    result = self.drCBAS.wait_for_ingestion(
                        cluster, self.data_sources[key], self.index_timeout)
                    self.assertTrue(result, "CBAS ingestion couldn't complete in time: %s" % self.index_timeout)

        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                for data_sources in self.data_sources.values():
                    for data_source in data_sources:
                        if data_source.loadDefn.get("cbasQPS", 0) > 0:
                            ql = CBASQueryLoad(cluster, data_source)
                            ql.start_query_load()
                            self.cbasQL.append(ql)

    def cluster_state_monitor(self, cluster):
        while not self.stop_run:
            try:
                status, content, _ = CBASHelper(cluster.master).get_cluster_details()
                if status:
                    cluster.cbas_cc_node.ip = json.loads(content)["ccNodeName"].split(":")[0]
                    cluster.master.ip = cluster.cbas_cc_node.ip
                    state = json.loads(content)["state"]
                    if state != "ACTIVE":
                        cluster.state = state
                        self.log.critical("Columnar cluster state is {}".format(state))
                    elif cluster.state != "ACTIVE":
                        self.log.critical("Columnar cluster state is {}".format(state))
                        cluster.state = state
            except Exception as e:
                print(e)
                import traceback
                traceback.print_exc()
            self.sleep(30)


    def test_rebalance(self):
        scaling = self.input.param("scaling", True)
        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                cpu_monitor = threading.Thread(target=self.print_cluster_cpu_ram,
                                               kwargs={"cluster": cluster})
                cpu_monitor.start()
                
                state_monitor = threading.Thread(target=self.cluster_state_monitor,
                                               kwargs={"cluster": cluster})
                state_monitor.start()
                
        self.infra_setup()
        self.sleep(0)
        self.mutate = 0
        loop = self.input.param("loop", 2)

        # start KV workload on couchbase remote clusters
        self.live_kv_workload()

        # start KV workload on Mongo clusters
        for dataSource in self.data_sources["mongo"]:
            self.generate_docs(bucket=dataSource)
            self.mongo_workload_tasks.extend(dataSource.perform_load(
                [dataSource], wait_for_load=False, tm=self.doc_loading_tm))

        while loop > 0:
            loop -= 1
            self.ingestion_ths = list()
            ingestion_result = list()

            # Create new collections
            for dataSource in self.data_sources["remoteCouchbase"] + self.data_sources["mongo"]:
                for tenant in self.tenants:
                    for columnar in tenant.columnar_instances:
                        self.drCBAS.disconnect_link(columnar, dataSource.link_name)
                        self.sleep(60, "wait after previous link disconnect")
                        self.cbcollect_logs(tenant, cluster.cluster_id, log_id="1")
                        dataSource.link_name = "{}_".format(dataSource.type) + ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(5)])
                        dataSource.links.append(dataSource.link_name)
                        dataSource.loadDefn.get("cbas")[1] = dataSource.loadDefn.get("cbas")[1] + self.default_workload.get("cbas")[1]
                        self.drCBAS.create_links(columnar, [dataSource])
                        th = threading.Thread(
                            target=self.drCBAS.wait_for_ingestion,
                            args=(columnar, [dataSource], self.index_timeout,
                                  ingestion_result))
                        th.start()
                        self.ingestion_ths.append(th)
            if scaling:
                self.sleep(300, "Wait for ingestion start before triggering scaling cycle")
                iterations = self.input.param("iterations", 1)
                nodes = self.num_nodes_in_columnar_instance
                for i in range(iterations-1, -1, -1):
                    self.PrintStep("Scaling IN operation: %s" % str(i+1))
                    tasks = list()
                    nodes = nodes/2
                    for tenant in self.tenants:
                        for cluster in tenant.columnar_instances:
                            self.cluster_util.print_cluster_stats(cluster)
                            _task = ScaleColumnarInstance(self.pod, tenant, cluster, nodes, timeout=self.wait_timeout)
                            self.task_manager.add_new_task(_task)
                            tasks.append(_task)
                    # self.wait_for_rebalances(tasks)
                    for task in tasks:
                        self.task_manager.get_task_result(task)
                        self.assertTrue(task.result, "Scaling IN columnar failed!")
                    self.sleep(1800, "Lets the ingestion/query running for 30 mins post scaling")
                for i in range(0, iterations):
                    self.PrintStep("Scaling OUT operation: %s" % str(i+1))
                    tasks = list()
                    nodes = nodes*2
                    for tenant in self.tenants:
                        for cluster in tenant.columnar_instances:
                            self.cluster_util.print_cluster_stats(cluster)
                            _task = ScaleColumnarInstance(self.pod, tenant, cluster,
                                                          nodes,
                                                          timeout=self.wait_timeout)
                            self.task_manager.add_new_task(_task)
                            tasks.append(_task)
                    # self.wait_for_rebalances(tasks)
                    for task in tasks:
                        self.task_manager.get_task_result(task)
                        self.assertTrue(task.result, "Scaling OUT columnar failed!")
                    self.sleep(1800, "Lets the ingestion/query running for 30 mins post scaling")
            for th in self.ingestion_ths:
                th.join()
            if False in ingestion_result:
                for tenant in self.tenants:
                    for columnar in tenant.columnar_instances:
                        self.cbcollect_logs(tenant, cluster.cluster_id, log_id="2")

        for ql in self.cbasQL:
            ql.stop_query_load()

        self.sleep(10, "Wait for 10s until all the query workload stops.")
        self.assertTrue(self.query_result, "Please check the logs for query failures")
