"""
Created on Feb 16, 2022

@author: ritesh.agarwal
"""
import copy
import json

from BucketLib.bucket import Bucket
from cb_constants import CbServer
from TestInput import TestInputSingleton, TestInputServer
from bucket_utils.bucket_ready_functions import BucketUtils
from capella_utils.dedicated import CapellaUtils
from capella_utils.common_utils import Pod, Tenant
from cb_basetest import CouchbaseBaseTest
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from constants.cloud_constants.capella_constants import AWS, Cluster
from security_config import trust_all_certs
from Jython_tasks.task import DeployCloud
import uuid
from table_view import TableView


class OnCloudBaseTest(CouchbaseBaseTest):
    def setUp(self):
        super(OnCloudBaseTest, self).setUp()

        # Cluster level info settings
        self.servers = list()
        self.capella = self.input.capella
        self.num_clusters = self.input.param("num_clusters", 1)

        # End of bucket parameters

        # Doc Loader Params (Extension from cb_basetest)
        self.delete_docs_at_end = self.input.param(
            "delete_doc_at_end", True)
        # End of client specific parameters

        self.wait_timeout = self.input.param("wait_timeout", 120)
        self.use_https = self.input.param("use_https", True)
        self.enforce_tls = self.input.param("enforce_tls", True)
        self.ipv4_only = self.input.param("ipv4_only", False)
        self.ipv6_only = self.input.param("ipv6_only", False)
        self.multiple_ca = self.input.param("multiple_ca", False)
        self.xdcr_remote_clusters = self.input.param("xdcr_remote_clusters", 0)
        self.diskAutoScaling = self.input.param("diskAutoScaling", True)
        self.services_map = {"data": "kv",
                             "kv": "kv",
                             "index": "index",
                             "2i": "index",
                             "query": "n1ql",
                             "n1ql": "n1ql",
                             "analytics": "cbas",
                             "cbas": "cbas",
                             "search": "fts",
                             "fts": "fts",
                             "eventing": "eventing"}
        provider = self.input.param("provider", AWS.__str__).lower()
        self.compute = {
            "data": self.input.param("kv_compute", AWS.ComputeNode.VCPU4_RAM16 if provider == "aws" else "n2-standard-4"),
            "query": self.input.param("n1ql_compute", AWS.ComputeNode.VCPU4_RAM16 if provider == "aws" else "n2-standard-4"),
            "index": self.input.param("gsi_compute", AWS.ComputeNode.VCPU4_RAM16 if provider == "aws" else "n2-standard-4"),
            "search": self.input.param("fts_compute", AWS.ComputeNode.VCPU4_RAM16 if provider == "aws" else "n2-standard-4"),
            "analytics": self.input.param("cbas_compute", AWS.ComputeNode.VCPU4_RAM16 if provider == "aws" else "n2-standard-4"),
            "eventing": self.input.param("eventing_compute", AWS.ComputeNode.VCPU4_RAM16 if provider == "aws" else "n2-standard-4")
            }
        aws_storage_range = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        aws_min_iops = [3000, 4370, 5740, 7110, 8480, 9850, 11220, 12590, 13960, 15330, 16000]
        self.iops = {
            "data": self.input.param("kv_iops", 3000),
            "query": self.input.param("n1ql_iops", 3000),
            "index": self.input.param("gsi_iops", 3000),
            "search": self.input.param("fts_iops", 3000),
            "analytics": self.input.param("cbas_iops", 3000),
            "eventing": self.input.param("eventing_iops", 3000)
            }
        self.disk = {
            "data": self.input.param("kv_disk", 200),
            "query": self.input.param("n1ql_disk", 200),
            "index": self.input.param("gsi_disk", 200),
            "search": self.input.param("fts_disk", 200),
            "analytics": self.input.param("cbas_disk", 200),
            "eventing": self.input.param("eventing_disk", 200)
            }
        for i, storage in enumerate(aws_storage_range):
            for service in ["data", "query", "index", "search", "analytics", "eventing"]:
                if self.disk[service] >= storage:
                    self.iops[service] = max(aws_min_iops[i+1], self.iops[service])
        self.num_nodes = {
            "data": self.input.param("kv_nodes", 3),
            "query": self.input.param("n1ql_nodes", 2),
            "index": self.input.param("gsi_nodes", 2),
            "search": self.input.param("fts_nodes", 2),
            "analytics": self.input.param("cbas_nodes", 2),
            "eventing": self.input.param("eventing_nodes", 2)
            }
        CbServer.use_https = True
        trust_all_certs()

        # initialise pod object
        url = self.input.capella.get("pod")
        self.pod = Pod("https://%s" % url,
                       self.input.capella.get("override_token",
                                              None))
        self.xdcr_cluster = None
        self.tenant = Tenant(self.input.capella.get("tenant_id"),
                             self.input.capella.get("capella_user"),
                             self.input.capella.get("capella_pwd"),
                             self.input.capella.get("secret_key"),
                             self.input.capella.get("access_key"))
        self.tenant.name = self.tenant.user.split("@")[0]
        if not (self.input.capella.get("access_key") and
                self.input.capella.get("secret_key")):
            self.log.info("Creating API keys for tenant...")
            resp = CapellaUtils.create_access_secret_key(
                self.pod, self.tenant, self.tenant.name)
            self.tenant.api_secret_key = resp["secret"]
            self.tenant.api_access_key = resp["access"]
            self.tenant.api_key_id = resp["id"]

        '''
        Be careful while using this flag.
        This is only and only for stand-alone tests.
        During bugs reproductions, when a crash is seen
        stop_server_on_crash will stop the server
        so that we can collect data/logs/dumps at the right time
        '''
        self.stop_server_on_crash = self.input.param("stop_server_on_crash",
                                                     False)
        self.collect_data = self.input.param("collect_data", False)
        self.validate_system_event_logs = \
            self.input.param("validate_sys_event_logs", False)

        self.nonroot = False
        self.crash_warning = self.input.param("crash_warning", False)
        self.rest_username = \
            TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = \
            TestInputSingleton.input.membase_settings.rest_password

        self.log_setup_status(self.__class__.__name__, "started")
        self.cluster_name_format = "C%s"
        self.xdcr_cluster_name_format = "XDCR%s"
        default_cluster_index = cluster_index = 1

        if self.input.capella.get("image"):
            self.generate_cluster_config_internal()
        else:
            self.generate_cluster_config()

        self.tenant.project_id = \
            TestInputSingleton.input.capella.get("project", None)
        if not self.tenant.project_id:
            CapellaUtils.create_project(self.pod, self.tenant, "a_taf_run")

        # Comma separated cluster_ids [Eg: 123-456-789,111-222-333,..]
        cluster_ids = TestInputSingleton.input.capella \
            .get("clusters", "")
        try:
            if cluster_ids:
                cluster_ids = cluster_ids.split(",")
                self.__get_existing_cluster_details(cluster_ids)
            else:
                tasks = list()
                for _ in range(self.num_clusters):
                    cluster_name = self.cluster_name_format % cluster_index
                    name = "clusterName" if self.capella_cluster_config.get("clusterName") else "name"
                    self.capella_cluster_config[name] = \
                        "%s_%s_%s" % (
                            self.tenant.user.split("@")[0].replace(".", "").replace("+", ""),
                            self.input.param("provider", "aws"),
                            cluster_name)
                    deploy_task = DeployCloud(self.pod, self.tenant, cluster_name,
                                              self.capella_cluster_config,
                                              timeout=self.wait_timeout)
                    self.task_manager.add_new_task(deploy_task)
                    tasks.append(deploy_task)
                    cluster_index += 1
                default_xdcr_cluster_index, xdcr_cluster_index = 1, 1
                for _ in range(self.xdcr_remote_clusters):
                    self.log.info("Will create the clusters required for XDCR replication.")
                    cluster_name = self.xdcr_cluster_name_format % xdcr_cluster_index
                    capella_config = copy.deepcopy(self.capella_cluster_config)
                    capella_config['name'] = \
                        "%s_%s_%s" % (
                            self.tenant.user.split("@")[0].replace(".", "").replace("+", ""),
                            self.input.param("provider", "aws"),
                            cluster_name)
                    self.log.info(capella_config)
                    deploy_task = DeployCloud(self.pod, self.tenant, cluster_name,
                                              capella_config,
                                              timeout=self.wait_timeout)
                    self.task_manager.add_new_task(deploy_task)
                    tasks.append(deploy_task)
                    xdcr_cluster_index += 1
                self.generate_cluster_config()
                for task in tasks:
                    self.task_manager.get_task_result(task)
                    self.assertTrue(task.result, "Cluster deployment failed!")
                    CapellaUtils.create_db_user(
                        self.pod, self.tenant, task.cluster_id,
                        self.rest_username, self.rest_password)
                    self.__populate_cluster_info(task.cluster_id, task.servers,
                                                 task.srv, task.name,
                                                 self.capella_cluster_config)

            # Initialize self.cluster with first available cluster as default
            if self.cb_clusters:
                self.cluster = self.cb_clusters[self.cluster_name_format
                                                % default_cluster_index]
                self.cluster.edition = "enterprise"
                self.servers = self.cluster.servers

            if self.xdcr_remote_clusters > 0:
                self.xdcr_cluster = self.cb_clusters[self.xdcr_cluster_name_format
                                                     % default_xdcr_cluster_index]
            self.cluster_util = ClusterUtils(self.task_manager)
            self.bucket_util = BucketUtils(self.cluster_util, self.task)
            for _, cluster in self.cb_clusters.items():
                self.cluster_util.print_cluster_stats(cluster)

            self.sleep(10)
        except Exception as e:
            self.log.critical(e)
            self.tearDown()
            raise Exception("SetUp Failed - {}".format(e))

    def tearDown(self):
        if self.is_test_failed() and self.get_cbcollect_info:
            for _, cluster in self.cb_clusters.items():
                CapellaUtils.trigger_log_collection(self.pod, self.tenant, cluster.id)
            for _, cluster in self.cb_clusters.items():
                table = TableView(self.log.info)
                table.add_row(["URL"])
                task = CapellaUtils.check_logs_collect_status(self.pod, self.tenant, cluster.id)
                for _, logInfo in sorted(task["perNode"].items()):
                    table.add_row([logInfo["url"]])
                table.display("Cluster: {}".format(cluster.id))

        self.shutdown_task_manager()
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()

        if self.skip_teardown_cleanup:
            return

        if not TestInputSingleton.input.capella.get("clusters", None):
            for name, cluster in self.cb_clusters.items():
                self.log.info("Destroying cluster: {}".format(name))
                CapellaUtils.destroy_cluster(cluster)
        if not TestInputSingleton.input.capella.get("project", None):
            CapellaUtils.delete_project(self.pod, self.tenant)

    def __get_existing_cluster_details(self, cluster_ids):
        cluster_index = 1
        for cluster_id in cluster_ids:
            cluster_name = self.cluster_name_format % cluster_index
            self.log.info("Fetching cluster details for: %s" % cluster_id)
            # CapellaUtils.wait_until_done(self.pod, self.tenant, cluster_id,
            #                              "Cluster not healthy")
            cluster_info = CapellaUtils.get_cluster_info(self.pod, self.tenant,
                                                         cluster_id)
            cluster_srv = cluster_info.get("endpointsSrv")
            CapellaUtils.allow_my_ip(self.pod, self.tenant, cluster_id)
            CapellaUtils.create_db_user(
                    self.pod, self.tenant, cluster_id,
                    self.rest_username, self.rest_password)
            servers = CapellaUtils.get_nodes(self.pod, self.tenant, cluster_id)
            self.__populate_cluster_info(cluster_id, servers, cluster_srv,
                                         cluster_name, cluster_info)
            self.__populate_cluster_buckets(self.cb_clusters[cluster_name])

    def __populate_cluster_info(self, cluster_id, servers, cluster_srv,
                                cluster_name, service_config):
        nodes = list()
        for server in servers:
            temp_server = TestInputServer()
            temp_server.ip = server.get("hostname")
            temp_server.hostname = server.get("hostname")
            temp_server.services = server.get("services")
            temp_server.port = "18091"
            temp_server.rest_username = self.rest_username
            temp_server.rest_password = self.rest_password
            temp_server.type = "dedicated"
            temp_server.memcached_port = "11207"
            nodes.append(temp_server)
        cluster = CBCluster(username=self.rest_username,
                            password=self.rest_password,
                            servers=[None] * 40)
        cluster.id = cluster_id
        cluster.srv = cluster_srv
        cluster.cluster_config = service_config
        cluster.pod = self.pod
        cluster.tenant = self.tenant
        cluster.type = "dedicated"

        for temp_server in nodes:
            cluster.nodes_in_cluster.append(temp_server)
            if "Data" in temp_server.services:
                cluster.kv_nodes.append(temp_server)
            if "Query" in temp_server.services:
                cluster.query_nodes.append(temp_server)
            if "Index" in temp_server.services:
                cluster.index_nodes.append(temp_server)
            if "Eventing" in temp_server.services:
                cluster.eventing_nodes.append(temp_server)
            if "Analytics" in temp_server.services:
                cluster.cbas_nodes.append(temp_server)
            if "Search" in temp_server.services:
                cluster.fts_nodes.append(temp_server)

        cluster.master = cluster.kv_nodes[0]
        self.tenant.clusters.update({cluster.id: cluster})
        self.cb_clusters[cluster_name] = cluster

    def __populate_cluster_buckets(self, cluster):
        self.log.debug("Fetching bucket details from cluster %s" % cluster.id)
        buckets = json.loads(CapellaUtils.get_all_buckets(cluster)
                             .content)["buckets"]["data"]
        for bucket in buckets:
            bucket = bucket["data"]
            bucket_obj = Bucket({
                Bucket.name: bucket["name"],
                Bucket.ramQuotaMB: bucket["memoryAllocationInMb"],
                Bucket.replicaNumber: bucket["replicas"],
                Bucket.conflictResolutionType:
                    bucket["bucketConflictResolution"],
                Bucket.flushEnabled: bucket["flush"],
                Bucket.durabilityMinLevel: bucket["durabilityLevel"],
                Bucket.maxTTL: bucket["timeToLive"],
            })
            bucket_obj.uuid = bucket["id"]
            bucket_obj.stats.itemCount = bucket["stats"]["itemCount"]
            bucket_obj.stats.memUsed = bucket["stats"]["memoryUsedInMib"]
            cluster.buckets.append(bucket_obj)

    def generate_cluster_config(self):
        provider = self.input.param("provider", AWS.__str__).lower()
        if provider == "aws":
            self.provider = "aws"
            self.package = "Developer Pro"
        elif provider == "gcp":
            self.provider = "hostedGCP"
            self.package = "Enterprise"
        self.capella_cluster_config = CapellaUtils.get_cluster_config(
            description="Amazing Cloud",
            single_az=False,
            provider=self.provider,
            region=self.input.param("region", AWS.Region.US_WEST_2),
            timezone=Cluster.Timezone.PT,
            plan=self.package,
            version=self.input.capella.get("server_version", None),
            cluster_name="taf_cluster")

        services = self.input.param("services", "data")
        for service_group in services.split("-"):
            service_group = sorted(service_group.split(":"))
            service = service_group[0]
            service_config = CapellaUtils.get_cluster_config_spec(
                provider=self.provider,
                services=[self.services_map[_service.lower()] for _service in service_group],
                count=self.num_nodes[service],
                compute=self.compute[service],
                storage_type=self.input.param("type", AWS.StorageType.GP3).lower(),
                storage_size_gb=self.disk[service],
                storage_iops=self.iops[service],
                diskAutoScaling=self.diskAutoScaling)
            if self.capella_cluster_config["provider"] \
                    != AWS.__str__:
                service_config["disk"].pop("iops")
            self.capella_cluster_config["specs"].append(service_config)

    def create_specs(self):
        provider = self.input.param("provider", "aws").lower()

        _type = AWS.StorageType.GP3 if provider == "aws" else "pd-ssd"
        storage_type = self.input.param("type", _type).lower()

        specs = []
        services = self.input.param("services", "data")
        for service_group in services.split("-"):
            services = sorted(service_group.split(":"))
            service = services[0]
            spec = {
                "count": self.num_nodes[service],
                "compute": {
                    "type": self.compute[service],
                },
                "services": [{"type": self.services_map[_service.lower()]} for _service in services],
                "disk": {
                    "type": storage_type,
                    "sizeInGb": self.disk[service]
                },
                "diskAutoScaling": {"enabled": self.diskAutoScaling}
            }
            if provider == "aws":
                spec["disk"]["iops"] = self.iops[service]
            specs.append(spec)
        return specs

    def generate_cluster_config_internal(self):
        specs = self.create_specs()
        provider = self.input.param("provider", AWS.__str__).lower()
        region = self.input.param("region", AWS.Region.US_WEST_2)
        self.log.info("Specs are {} . Provider is {}. Region is {}".format(specs, provider, region))
        if provider == "aws":
            provider = "hostedAWS"
            package = "developerPro"
        elif provider == "gcp":
            provider = "hostedGCP"
            package = "enterprise"
        else:
            raise Exception("Provider has to be one of aws or gcp")
        self.capella_cluster_config = {
            "region": self.input.param("region", region),
            "provider": provider,
            "name": str(uuid.uuid4()),
            "cidr": None,
            "singleAZ": False,
            "specs": specs,
            "package": package,
            "projectId": None,
            "description": "",
        }

        if self.input.capella.get("image"):
            image = self.input.capella["image"]
            token = self.input.capella["override_token"]
            server_version = self.input.capella["server_version"]
            release_id = self.input.capella.get("release_id", None)
            self.capella_cluster_config["overRide"] = {"token": token,
                                                       "image": image,
                                                       "server": server_version}
            if release_id:
                self.capella_cluster_config["overRide"]["releaseId"] = release_id


class ClusterSetup(OnCloudBaseTest):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "started", "setup")

        # Print cluster stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()
