'''
Created on May 2, 2022

@author: ritesh.agarwal
'''
import threading
import time

from CbasUtil import DoctorCBAS, CBASQueryLoad
from aGoodDoctor.hostedOPD import OPD
from basetestcase import BaseTestCase
from table_view import TableView
from datasources import MongoDB, MongoWorkload
import string
import random
from com.couchbase.test.sdk import Server
from com.couchbase.test.sdk import SDKClient
from _collections import defaultdict


class Columnar(BaseTestCase, OPD):

    # def threads_calculation(self):
    #     self.process_concurrency = self.input.param("pc", self.process_concurrency)
    #     self.doc_loading_tm = TaskManager(self.process_concurrency)

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
        self.index_timeout = self.input.param("index_timeout", 3600)
        self.load_defn = list()
        self.cbasQL = list()
        self.stop_run = False
        self.skip_init = self.input.param("skip_init", False)
        self.query_result = True
        self.default_workload = {
            "valType": "Hotel",
            "database": 1,
            "collections": 1,
            "scopes": 1,
            "num_items": self.input.param("num_items", 10000),
            "start": 0,
            "end": self.input.param("num_items", 10000),
            "ops": self.input.param("ops_rate", 10000),
            "doc_size": 1024,
            "pattern": [0, 0, 100, 0, 0], # CRUDE
            "load_type": ["update"],
            "cbasQPS": 10,
            "cbas": [10, 10]
            }

        self.data_sources = defaultdict(list)
        self.mutation_perc = self.input.param("mutation_perc", 100)

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

        mongo = MongoDB(mongo_hostname, mongo_username,
                        mongo_password, mongo_atlas)
        mongo.name = "Mongo_" + ''.join([random.choice(string.ascii_letters + string.digits) for i in range(5)])
        mongo.loadDefn = self.default_workload
        mongo.set_mongo_collections()
        mongo.key = "test_docs-"
        mongo.key_size = 18
        mongo.key_type = "Circular"
        self.mongo_workload = MongoWorkload()
        self.generate_docs(doc_ops=["create"],
                           create_start=0,
                           create_end=mongo.loadDefn.get("num_items"),
                           bucket=mongo)
        self.data_sources["mongo"].append(mongo)

    def setup_sdk_clients(self, cluster):
        cluster.SDKClients = list()
        nebula_endpoint = cluster.nebula.endpoint
        master = Server(nebula_endpoint.ip, nebula_endpoint.port,
                        nebula_endpoint.rest_username, nebula_endpoint.rest_password,
                        str(nebula_endpoint.memcached_port))
        master.ip = "couchbases://" + master.ip + ":16001"
        client = SDKClient(master, "None")
        client.connectCluster()
        cluster.SDKClients.append(client)

    def tearDown(self):
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for data_source in self.data_sources["mongo"]:
                    self.drCBAS.disconnect_link(cluster, data_source.link_name)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for data_source in self.data_sources["mongo"]:
                    self.drCBAS.wait_for_link_disconnect(cluster, data_source.link_name, 3600)

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self.drCBAS.drop_collections(cluster, self.data_sources["mongo"])
                self.drCBAS.drop_links(cluster, self.data_sources["mongo"])

        self.check_dump_thread = False
        self.stop_crash = True
        self.stop_run = True
        for ql in self.cbasQL:
            ql.stop_query_load()
        self.sleep(10)
        BaseTestCase.tearDown(self)

    def monitor_query_status(self, print_duration=120):
        self.query_result = True

        def check_query_stats():
            st_time = time.time()
            while not self.stop_run:
                if st_time + print_duration < time.time():
                    self.CBAStable = TableView(self.log.info)
                    self.CBAStable.set_headers(["Bucket",
                                                "Total Queries",
                                                "Failed Queries",
                                                "Success Queries",
                                                "Rejected Queries",
                                                "Cancelled Queries",
                                                "Timeout Queries",
                                                "Errored Queries"])
                    for ql in self.cbasQL:
                        self.CBAStable.add_row([
                            str(ql.bucket.name),
                            str(ql.total_query_count),
                            str(ql.failed_count),
                            str(ql.success_count),
                            str(ql.rejected_count),
                            str(ql.cancel_count),
                            str(ql.timeout_count),
                            str(ql.error_count),
                            ])
                        if ql.failures > 0:
                            self.query_result = False
                    self.CBAStable.display("CBAS Query Statistics")

                    st_time = time.time()
                    time.sleep(10)

        query_monitor = threading.Thread(target=check_query_stats)
        query_monitor.start()

    def initial_setup(self):
        self.monitor_query_status()

        if self.input.param("onPremMongo", False):
            self.setupMongo(atlas=False)
        if self.input.param("onCloudMongo", False):
            self.setupMongo(atlas=True)

        self.mongo_workload.perform_load(self.data_sources["mongo"], wait_for_load=True,
                                         overRidePattern=[100, 0, 0, 0, 0],
                                         tm=self.doc_loading_tm)
        self.drCBAS = DoctorCBAS()
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self.setup_sdk_clients(cluster)
                self.drCBAS.create_mongo_links(cluster, self.data_sources["mongo"])
                self.sleep(60)

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for data_source in self.data_sources["mongo"]:
                    self.drCBAS.wait_for_link_connect(cluster, data_source.link_name, 3600)

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for key in self.data_sources.keys():
                    result = self.drCBAS.wait_for_ingestion(
                        cluster, self.data_sources[key], self.index_timeout)
                    self.assertTrue(result, "CBAS ingestion couldn't complete in time: %s" % self.index_timeout)

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for data_sources in self.data_sources.values():
                    for data_source in data_sources:
                        if data_source.loadDefn.get("cbasQPS", 0) > 0:
                            ql = CBASQueryLoad(cluster, data_source)
                            ql.start_query_load()
                            self.cbasQL.append(ql)

    def test_rebalance(self):
        self.initial_setup()
        self.sleep(30)
        self.mutate = 0
        for datasources in self.data_sources.values():
            for datasource in datasources:
                self.generate_docs(bucket=datasource)

        self.mongo_workload.perform_load(self.data_sources["mongo"],
                                         wait_for_load=False,
                                         tm=self.doc_loading_tm)
        for i in range(2, 6):
            self.PrintStep("Scaling operation: %s" % str(i-1))
            for tenant in self.tenants:
                for cluster in tenant.clusters:
                    self.goldfish_utils.scale_cluster(self.pod, tenant, cluster, i)
            for tenant in self.tenants:
                for cluster in tenant.clusters:
                    self.goldfish_utils.wait_for_cluster_scaling(self.pod, tenant, cluster)
            self.sleep(600)
        for i in range(5, 1):
            self.PrintStep("Scaling operation: %s" % str(i-1))
            for tenant in self.tenants:
                for cluster in tenant.clusters:
                    self.goldfish_utils.scale_cluster(self.pod, tenant, cluster, i)
            for tenant in self.tenants:
                for cluster in tenant.clusters:
                    self.goldfish_utils.wait_for_cluster_scaling(self.pod, tenant, cluster)
            self.sleep(600)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                for ql in self.cbasQL:
                    ql.stop_query_load()

        self.sleep(10, "Wait for 10s until all the query workload stops.")
        self.assertTrue(self.query_result, "Please check the logs for query failures")
