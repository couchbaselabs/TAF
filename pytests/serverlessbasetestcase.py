"""
Created on Feb 16, 2022

@author: ritesh.agarwal
"""
from cb_constants import CbServer
from Jython_tasks.task import DeployDataplane
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import BucketUtils, DocLoaderUtils
from capella_utils.common_utils import Pod, Tenant
from capella_utils.dedicated import CapellaUtils as DedicatedUtils
from capella_utils.serverless import CapellaUtils as ServerlessUtils, \
    CapellaUtils
from cb_basetest import CouchbaseBaseTest
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster,\
    Dataplane
from constants.cloud_constants.capella_constants import AWS
from threading import Thread
import global_vars
from uuid import uuid4
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI
from membase.api.rest_client import RestConnection
from table_view import TableView
import urllib
import re


class OnCloudBaseTest(CouchbaseBaseTest):
    def setUp(self):
        super(OnCloudBaseTest, self).setUp()

        for server in self.input.servers:
            server.type = "serverless"
        # End of framework parameters

        # Cluster level info settings
        self.servers = list()
        self.capella = self.input.capella

        self.wait_timeout = self.input.param("wait_timeout", 1800)
        CbServer.use_https = True

        # initialize pod object
        url = self.input.capella.get("pod")
        self.pod = Pod("https://%s" % url,
                       self.input.capella.get("token",
                                              None))
        # Set this variable to determine whether the test are running on
        # production pipeline where '/internal' endpoints are prohibited
        if "stage" in self.pod.url_public \
                or "cloud.couchbase.com" in self.pod.url_public:
            CapellaUtils.is_prod_env = True

        self.log_setup_status(self.__class__.__name__, "started")
        self.cluster_name_format = "C%s"
        self.nebula_details = dict()

        self.rest_username = \
            TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = \
            TestInputSingleton.input.membase_settings.rest_password

        self.tenants = list()
        self.signup_token = self.input.capella.get("signup_token")
        num_tenants = self.input.param("num_tenants", 0)

        if self.input.capella.get("tenant_id"):
            self.tenant = Tenant(self.input.capella.get("tenant_id"),
                                 self.input.capella.get("capella_user"),
                                 self.input.capella.get("capella_pwd"))

            self.tenant.project_id = \
                TestInputSingleton.input.capella.get("project", None)
            if not self.tenant.project_id:
                DedicatedUtils.create_project(self.pod, self.tenant, "a_taf_run")
            self.tenants.append(self.tenant)
        self.create_tenants(url, num_tenants)

        self.cluster = CBCluster(username=self.rest_username,
                                 password=self.rest_password,
                                 servers=[None] * 40)
        self.cluster.pod = self.pod
        self.cluster.tenant = self.tenants[0]
        self.cluster.type = "serverless"

        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)
        self.serverless_util = ServerlessUtils(self.cluster)

        create_dataplane=self.input.param("create_dataplane", False)
        num_dataplanes = self.input.param("num_dataplanes", 0)
        if create_dataplane:
            num_dataplanes += 1

        # Comma separated cluster_ids [Eg: 123-456-789,111-222-333,..]
        self.dataplanes = self.input.capella.get("dataplane_id")
        if self.dataplanes:
            self.dataplanes = self.dataplanes.split(",")
            self.dataplane_id = self.dataplanes[-1]
        else:
            self.dataplanes = list()
            self.dataplane_id = ""

        self.delete_dataplanes = list()

        tasks = list()
        for _ in range(num_dataplanes):
            self.generate_dataplane_config()
            self.log.info(self.dataplane_config)
            deploy_task = DeployDataplane(self.cluster,
                                          self.dataplane_config,
                                          timeout=self.wait_timeout)
            self.task_manager.add_new_task(deploy_task)
            tasks.append(deploy_task)
            self.sleep(10)
        for deploy_task in tasks:
            self.task_manager.get_task_result(deploy_task)
            self.assertTrue(deploy_task.result, "Dataplane deployment failed!")
            self.dataplanes.append(deploy_task.dataplane_id)
            self.delete_dataplanes.append(deploy_task.dataplane_id)
            self.dataplane_id = deploy_task.dataplane_id

        self.dataplane_objs = dict()
        self.table = TableView(self.log.info)
        for dataplane_id in self.dataplanes:
            self.table.set_headers(["Dataplane",
                                    "SRV",
                                    "IP",
                                    "Username",
                                    "Password",
                                    "Nodes"])
            self.log.info("Bypassing dataplane: {}".format(dataplane_id))
            srv, ip, user, pwd = self.serverless_util.bypass_dataplane(dataplane_id)
            if "" in [ip,user,pwd]:
                continue
            dataplane = Dataplane(dataplane_id, srv, user, pwd)
            dataplane.cluster_id = self.serverless_util.get_dataplane_clusterid(dataplane_id)
            rest = RestConnection({"ip": ip,
                                   "username": user,
                                   "password": pwd,
                                   "port": 18091})
            servers = rest.get_nodes()
            dataplane.refresh_object(servers)
            self.dataplane_objs.update({dataplane.id: dataplane})
            text = "\n\nDataplane - {}:".format(dataplane_id)
            text += "\nKV nodes: {}".format(dataplane.kv_nodes)
            text += "\nIndex nodes: {}".format(dataplane.index_nodes)
            text += "\nQuery nodes: {}".format(dataplane.query_nodes)
            text += "\nFTS nodes: {}\n\n".format(dataplane.fts_nodes)
            self.table.add_row([
                str(dataplane_id),
                str(srv),
                str(ip),
                str(user),
                str(pwd),
                ])
            throttling_limit = self.input.param("throttling_limit", False)
            if throttling_limit is not False:
                api = rest.baseUrl + "internalSettings"
                params = urllib.urlencode({'dataThrottleLimit': throttling_limit,
                                           'indexThrottleLimit': throttling_limit,
                                           'searchThrottleLimit': throttling_limit,
                                           'queryThrottleLimit': throttling_limit})
                status, content, _ = rest._http_request(
                    api, "POST", params=params,
                    headers=rest.get_headers_for_content_type_json())
                self.log.info("Throttling Off: {}, {}".format(status, content))
                self.assertTrue(status, "Turning throttling off failed: {}".format(status))
        if self.dataplanes:
            self.table.display("Dataplanes")
        # Setting global_vars for future reference
        global_vars.cluster_util = self.cluster_util
        global_vars.bucket_util = self.bucket_util
        global_vars.serverless_util = self.serverless_util

        self.__init_collection_specific_params()

    def create_tenants(self, url, num_tenants):
        if num_tenants == 0:
            return

        def seed_email(email):
            uuid = uuid4()
            a, b = email.split("@")
            return "{}".format(uuid), "{}+{}@{}".format(a, uuid, b)

        email = self.input.capella.get("capella_user")
        self.commonAPI = CommonCapellaAPI(
            "https://{}".format(url), None, None,
            self.input.capella.get("capella_user"), None)

        self.log.info("Singup token: {0}".format(self.signup_token))

        for _ in range(num_tenants):
            full_name, seed_mail = seed_email(email)
            #seed_pwd = self.input.capella.get("capella_pwd", "Couch@123")
            # resp = self.commonAPI.signup_user(full_name, seed_mail, seed_pwd,
            #                                   full_name, self.signup_token)
            # verify_token = resp.headers["Vnd-project-Avengers-com-e2e-token"]
            # tenant_id = resp.json()["tenantId"]
            # resp = self.commonAPI.verify_email(verify_token)
            seed_pwd = str(uuid4()) + "!1Aa"
            resp = self.commonAPI.signup_user(full_name, seed_mail, seed_pwd,
                                              full_name, self.signup_token)
            resp.raise_for_status()
            verify_token = resp.headers["Vnd-project-Avengers-com-e2e-token"]
            user_id = resp.json()["userId"]
            tenant_id = resp.json()["tenantId"]

            resp = self.commonAPI.verify_email(verify_token)
            resp.raise_for_status()
            tenant = Tenant(tenant_id,
                            seed_mail,
                            seed_pwd)
            self.tenants.append(tenant)
            self.log.info("Tenant Created - tenantID: {}, user: {}, pwd: {}".format(
                tenant_id, seed_mail, seed_pwd))
            self.log.info("Creating project for the above tenant")
            DedicatedUtils.create_project(self.pod, tenant, "a_taf_run")

    def __init_collection_specific_params(self):
        self.spec_name = self.input.param("bucket_spec", None)
        self.data_spec_name = self.input.param("data_spec_name", None)

    def tearDown(self):
        self.shutdown_task_manager()
        if self.cluster.sdk_client_pool:
            self.cluster.sdk_client_pool.shutdown()

        if self.is_test_failed() and self.get_cbcollect_info:
            for dataplane in self.dataplane_objs.values():
                self.serverless_util.trigger_log_collection(dataplane.cluster_id)
            for dataplane in self.dataplane_objs.values():
                table = TableView(self.log.info)
                table.add_row(["URL"])
                while True:
                    tasks = self.serverless_util.get_cluster_tasks(dataplane.cluster_id)
                    task = [task for task in tasks if task["type"] == "clusterLogsCollection"][0]
                    self.log.info("Logs on Dataplane {}: Status {} - Progress {}%".
                                  format(dataplane.id, task["status"], task["progress"]))
                    if task["status"] == "completed":
                        for _, logInfo in sorted(task["perNode"].items()):
                            table.add_row([logInfo["url"]])
                        table.display("Dataplane: {}".format(dataplane.id))
                        break
                    self.sleep(10)

        if self.skip_teardown_cleanup:
            return
        for bucket in self.cluster.buckets:
            self.log.info("Deleting database: {}".format(bucket.name))
            self.serverless_util.delete_database(self.pod, self.tenant, bucket.name)

        db_delete_wait_thread = []
        for bucket in self.cluster.buckets:
            wait_thread = Thread(
                target=self.serverless_util.wait_for_database_deleted,
                name="waiting_thread_{0}".format(
                    bucket.name),
                args=(self.tenant, bucket.name, 600))
            db_delete_wait_thread.append(wait_thread)
            wait_thread.start()

        for wait_thread in db_delete_wait_thread:
            wait_thread.join()

        for dataplane_id in self.delete_dataplanes:
            self.log.info("Destroying dataplane: {}".format(dataplane_id))
            self.serverless_util.delete_dataplane(dataplane_id)
        if not TestInputSingleton.input.capella.get("project", None):
            DedicatedUtils.delete_project(self.pod, self.tenant)

    def generate_dataplane_config(self):
        cb_image = self.input.capella.get("cb_image", "")
        dapi_image = self.input.capella.get("dapi_image", "")
        dn_image = self.input.capella.get("dn_image", "")

        if not(cb_image or dn_image or dapi_image):
            raise Exception("Please provide atleast one image while deploying a dataplane.")
        provider = self.input.param("provider", AWS.__str__).lower()
        region = self.input.param("region", AWS.Region.US_EAST_1)

        cb_version = ""
        if cb_image:
            version_split = cb_image.split("-")
            for _split in version_split:
                if re.search("^\d\.\d\.\d", _split):
                    cb_version = _split
                    break
        services_type = self.input.param("services", "kv:n1ql:fts:index")
        services_type = services_type.split(":")
        disk_type = self.input.param("disk_type", "gp3")
        kv_disk_size = self.input.param("kv_disk_size", 500)
        index_disk_size = self.input.param("index_disk_size", 400)
        n1ql_disk_size = self.input.param("n1ql_disk_size", 50)
        fts_disk_size = self.input.param("fts_disk_size", 400)
        disk_size = {"kv": kv_disk_size,
                     "index": index_disk_size,
                     "n1ql": n1ql_disk_size,
                     "fts": fts_disk_size}
        nodes = {"kv": self.input.param("kv_nodes", 3),
                 "index": self.input.param("index_nodes", 2),
                 "n1ql": self.input.param("n1ql_nodes", 2),
                 "fts": self.input.param("fts_nodes", 2)}
        compute = {"kv": self.input.param("kv_compute", "c6gd.2xlarge"),
                   "index": self.input.param("gsi_compute", "c6gd.4xlarge"),
                   "n1ql": self.input.param("n1ql_compute", "c6g.4xlarge"),
                   "fts": self.input.param("fts_compute", "c6gd.4xlarge")}
        disk_iops = self.input.param("disk_iops", 3000)

        self.dataplane_config = {
            "provider": provider,
            "region": region,
            "overRide": {
                "couchbase": {
                    "image": cb_image,
                    "version": cb_version
                }
            }
        }
        if services_type:
            self.dataplane_config["overRide"]["couchbase"]["specs"] = list()
        for service in services_type:
            spec = dict()
            spec["services"] = [{"type": service}]
            spec["compute"] = {"type": compute[service]}
            spec["count"] = nodes[service]
            spec["disk"] = dict()
            if disk_type:
                spec["disk"]["type"] = disk_type
            if disk_size:
                spec["disk"]["sizeInGb"] = disk_size[service]
            if disk_iops:
                spec["disk"]["iops"] = disk_iops
            self.dataplane_config["overRide"]["couchbase"]["specs"].append(spec)

        if dn_image:
            self.dataplane_config["overRide"].update(
                {
                    "nebula": {
                        "image": dn_image
                        }
                    }
                )
        if dapi_image:
            self.dataplane_config["overRide"].update(
                {
                    "dataApi": {
                        "image": dapi_image
                        }
                    }
                )


class ClusterSetup(OnCloudBaseTest):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "started", "setup")

        # Print cluster stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()
