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
from datasources import MongoDB
from com.couchbase.test.sdk import Server
from com.couchbase.test.sdk import SDKClient
from _collections import defaultdict
from datasources import s3
from capella_utils.dedicated import CapellaUtils
from Jython_tasks.task import ScaleColumnarInstance


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
        mongo.loadDefn = self.default_workload
        mongo.set_collections()
        mongo.key = "test_docs-"
        mongo.key_size = 18
        mongo.key_type = "Circular"
        self.generate_docs(doc_ops=["create"],
                           create_start=0,
                           create_end=mongo.loadDefn.get("num_items"),
                           bucket=mongo)
        self.data_sources["mongo"].append(mongo)

        mongo.perform_load(self.data_sources["mongo"], wait_for_load=True,
                           overRidePattern=[100, 0, 0, 0, 0],
                           tm=self.doc_loading_tm)

    def teardownMongo(self):
        for database in self.data_sources["mongo"]:
            for task in database.tasks:
                task.sdk.dropDatabase()
                task.sdk.disconnectCluster()
                return

    def setup_sdk_clients(self, cluster):
        cluster.SDKClients = list()
        master = Server(cluster.srv, cluster.master.port,
                        cluster.master.rest_username, cluster.master.rest_password,
                        str(cluster.master.memcached_port))
        client = SDKClient(master, "None")
        client.connectCluster()
        cluster.SDKClients.append(client)

    def tearDown(self):
        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                for data_source in self.data_sources["mongo"]:
                    self.drCBAS.disconnect_link(cluster, data_source.link_name)
        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                for data_source in self.data_sources["mongo"]:
                    self.drCBAS.wait_for_link_disconnect(cluster, data_source.link_name, 3600)

        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
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

        s3_obj = s3(self.input.datasources.get("s3_access"),
                    self.input.datasources.get("s3_secret"))
        s3_obj.loadDefn = self.default_workload
        s3_obj.set_collections()
        self.data_sources["s3"].append(s3_obj)

        self.drCBAS = DoctorCBAS()
        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                self.setup_sdk_clients(cluster)
                for datasources in self.data_sources.values():
                    self.drCBAS.create_links(cluster, datasources)
                    # self.sleep(60)

        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                for data_source in self.data_sources["mongo"]:
                    self.drCBAS.wait_for_link_connect(cluster, data_source.link_name, 3600)

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

    def test_rebalance(self):
        self.initial_setup()
        self.sleep(300)
        self.mutate = 0
        self.workload_tasks = list()
        for key in self.data_sources.keys():
            if key == "s3":
                continue
            for dataSource in self.data_sources[key]:
                self.generate_docs(bucket=dataSource)
                self.workload_tasks.append(dataSource.perform_load(
                    [dataSource], wait_for_load=False, tm=self.doc_loading_tm))
        self.iterations = self.input.param("iterations", 1)
        nodes = self.num_nodes_in_columnar_instance
        for i in range(0, self.iterations):
            self.PrintStep("Scaling OUT operation: %s" % str(i+1))
            tasks = list()
            nodes = nodes*2
            for tenant in self.tenants:
                for cluster in tenant.columnar_instances:
                    servers = CapellaUtils.get_nodes(self.pod, tenant, cluster.cluster_id)
                    tbl = TableView(self.log.info)
                    tbl.set_headers(["Hostname", "Services"])
                    for server in servers:
                        tbl.add_row([server.get("hostname"), server.get("services")])
                    tbl.display("Nodes in instance/cluster: {}/{}".format(cluster.instance_id, cluster.cluster_id))
                    _task = ScaleColumnarInstance(self.pod, tenant, cluster,
                                                  nodes,
                                                  timeout=self.wait_timeout)
                    self.task_manager.add_new_task(_task)
                    tasks.append(_task)
            for task in tasks:
                self.task_manager.get_task_result(task)
                self.assertTrue(task.result, "Cluster deployment failed!")
            self.sleep(60)
        for i in range(self.iterations-1, -1, -1):
            self.PrintStep("Scaling IN operation: %s" % str(i))
            tasks = list()
            nodes = nodes/2
            for tenant in self.tenants:
                for cluster in tenant.columnar_instances:
                    servers = CapellaUtils.get_nodes(self.pod, tenant, cluster.cluster_id)
                    tbl = TableView(self.log.info)
                    tbl.set_headers(["Hostname", "Services"])
                    for server in servers:
                        tbl.add_row([server.get("hostname"), server.get("services")])
                    tbl.display("Nodes in instance/cluster: {}/{}".format(cluster.instance_id, cluster.cluster_id))
                    _task = ScaleColumnarInstance(self.pod, tenant, cluster, nodes, timeout=self.wait_timeout)
                    self.task_manager.add_new_task(_task)
                    tasks.append(_task)
            for task in tasks:
                self.task_manager.get_task_result(task)
                self.assertTrue(task.result, "Cluster deployment failed!")
            self.sleep(600)
        for tenant in self.tenants:
            for cluster in tenant.columnar_instances:
                for ql in self.cbasQL:
                    ql.stop_query_load()

        self.teardownMongo()
        self.sleep(10, "Wait for 10s until all the query workload stops.")
        self.assertTrue(self.query_result, "Please check the logs for query failures")
