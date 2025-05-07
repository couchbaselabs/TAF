'''
Created on 04-Jun-2021
@author: Umang Agrawal
'''

from basetestcase import BaseTestCase
from TestInput import TestInputSingleton
from cluster_utils.cluster_ready_functions import CBCluster
from cb_constants import CbServer
from cbas_utils.cbas_utils import CbasUtil
from bucket_collections.collections_base import CollectionBase
from collections_helper.collections_spec_constants import MetaConstants
from BucketLib.bucket import Bucket
from security_utils.security_utils import SecurityUtils
from tpch_utils.tpch_utils import TPCHUtil
from couchbase_utils.security_utils.x509_multiple_CA_util import x509main
from sdk_client3 import SDKClient
from cb_server_rest_util.rest_client import RestConnection
import time
from awsLib.S3 import S3
from cb_server_rest_util.analytics.analytics_api import AnalyticsRestAPI
from columnarbasetestcase import ColumnarBaseTest


class CBASBaseTest(BaseTestCase):

    def setUp(self):
        """
        Since BaseTestCase will initialize at least one cluster, we pass service
        for the master node of that cluster
        """
        if not hasattr(self, "input"):
            self.input = TestInputSingleton.input

        """ In case of multi cluster setup, if cluster address family needs to be
        set, then this parameter is required """
        if self.input.param("cluster_ip_family", ""):
            cluster_ip_family = self.input.param("cluster_ip_family", "").split("|")
            if cluster_ip_family[0] == "ipv4_only":
                self.input.test_params.update({
                "ipv4_only": True, "ipv6_only": False})
            elif cluster_ip_family[0] == "ipv6_only":
                self.input.test_params.update({
                "ipv4_only": False, "ipv6_only": True})
            elif cluster_ip_family[0] == "ipv4_ipv6":
                self.input.test_params.update({
                "ipv4_only": True, "ipv6_only": True})
            else:
                self.input.test_params.update({
                "ipv4_only": False, "ipv6_only": False})

        """
        Cluster node services. Parameter value format
        serv1:serv2-serv1:ser2|serv1:serv2-ser1:serv2
        | -> separates services per cluster.
        - -> separates services on each node of the cluster.
        : -> separates services on a node.
        """
        services_init = [x for x in self.input.param(
            "services_init", "kv:n1ql:index").split("|")]
        if len(services_init) > 1:
            self.input.test_params["services_init"] = services_init[0]

        super(CBASBaseTest, self).setUp()

        """
        Cluster node services. Parameter value format
        serv1:serv2-serv1:ser2|serv1:serv2-ser1:serv2
        | -> separates services per cluster.
        - -> separates services on each node of the cluster.
        : -> separates services on a node.
        """
        self.services_init = [x.split("-") for x in services_init]

        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)

        """
        Parameterized Support for multiple cluster instead of creating
        multiple clusters from ini file.
        """
        self.num_of_clusters = self.input.param('num_of_clusters', 1)

        """
        For columnar on-prem tests
        """
        if self.input.param("runtype", "default") == "onprem-columnar":
            self.analytics_compute_storage_separation = True

        """
        Since BaseTestCase will initialize at least one cluster, we need to
        modify the initialized cluster server property to correctly reflect the
        servers in that cluster.
        """
        cluster = self.cb_clusters[list(self.cb_clusters.keys())[0]]
        if self.num_of_clusters == 1:
            if cluster.master.type != "columnar":
                self.num_of_clusters = 2
                cluster.servers = [
                    server for server in self.servers if server.type == "default"]
            elif self.input.param("runtype", "default") == "onprem-columnar":
                cluster.servers = [
                    server for server in self.servers if server.type == "columnar"]

        if "cbas" in cluster.master.services:
            cluster.cbas_nodes.append(cluster.master)

        cluster.rest = RestConnection(cluster.master)
        cluster.rest.activate_service_api(["cbas", "security"])

        """
        Number of nodes per cluster. Parameter value format
        num_nodes_cluster1|num_nodes_cluster2|....
        | -> separates number of nodes per cluster.
        """
        if not isinstance(self.input.param("nodes_init", 1), int):
            self.nodes_init = [int(x) for x in self.input.param(
                "nodes_init", 1).split("|")]
        elif self.num_of_clusters > 1:
            self.nodes_init = [1] * self.num_of_clusters
        else:
            self.nodes_init = [self.input.param("nodes_init", 1)]


        """
        For the remote cluster set max memory for KV
        """
        if self.num_of_clusters > 1:
            service_memory_config = dict()
            _, info = cluster.rest.cluster.node_details()
            total_memory = int(info["mcdMemoryReserved"] * 0.8)
            if "index" in cluster.master.services:
                service_memory_config[CbServer.Settings.INDEX_MEM_QUOTA] \
                    = CbServer.Settings.MinRAMQuota.INDEX
                total_memory -= service_memory_config[
                    CbServer.Settings.INDEX_MEM_QUOTA]
            service_memory_config[CbServer.Settings.KV_MEM_QUOTA] = total_memory
            status, content = cluster.rest.cluster.configure_memory(
                service_memory_config)
            if not status:
                self.fail("Unable to modify memory setting for remote cluster")

        # Force disable TLS to avoid initial connection issues
        # tasks = [self.node_utils.async_disable_tls(server)
        #          for server in self.servers]
        # for task in tasks:
        #     self.task_manager.get_task_result(task)
        # for server in self.servers:
        #     self.set_ports_for_server(server, "non_ssl")
        # CbServer.use_https = False

        """
        Since BaseTestCase will initialize at least one cluster, we need to
        initialize only total clusters required - 1.
        """
        """
        In case of On-Prem columnar tests, make sure to specify remote 
        cluster services first in conf file followed by services for analytics 
        cluster i.e. services_init=kv:n1ql:index-kv:n1ql:index(remote 
        cluster)|kv:cbas-cbas(analytics cluster), this is required since we 
        initialize the first cluster in onPrem_basetestcase file. The 
        analytics cluster will be initialised below with or without compute 
        storage separation based on runtype parameter. 
        In order to setup analytics cluster with compute storage separation, 
        the runtype should be passed as onprem-columnar 
        """
        cluster_name_format = "C%s"
        for i in range(1, self.num_of_clusters):
            # Construct dict of mem. quota percent / mb per service
            mem_quota_percent = dict()


            servers = [
                server for server in self.servers if server.type == "columnar"]
            cluster_name = cluster_name_format % str(i+1)
            cluster = CBCluster(
                name=cluster_name,
                servers=servers)
            self.cb_clusters[cluster_name] = cluster
            cluster.kv_nodes.append(cluster.master)

            if self.analytics_compute_storage_separation:
                # Construct dict of mem. quota percent per service
                mem_quota_percent[CbServer.Services.CBAS] = 80
                mem_quota_percent[CbServer.Services.KV] = 10

            services = "kv:cbas"
            # if self.input.param("runtype", "default") == "onprem-columnar":
            #     services = ""
            self.initialize_cluster(
                cluster_name, cluster, services=services,
                services_mem_quota_percent=mem_quota_percent
            )
            cluster.master.services = ["kv", "cbas"]

            if "cbas" in cluster.master.services or "columnar" in cluster.master.services:
                cluster.cbas_nodes.append(cluster.master)

            if self.input.param("cluster_ip_family", ""):
                # Enforce IPv4 or IPv6 or both
                if cluster_ip_family[i] == "ipv4_only":
                    status, msg = self.cluster_util.enable_disable_ip_address_family_type(
                        cluster, True, True, False)
                if cluster_ip_family[i] == "ipv6_only":
                    status, msg = self.cluster_util.enable_disable_ip_address_family_type(
                        cluster, True, False, True)
                if cluster_ip_family[i] == "ipv4_ipv6":
                    status, msg = self.cluster_util.enable_disable_ip_address_family_type(
                        cluster, True, True, True)
                if not status:
                    self.fail(msg)
            self.modify_cluster_settings(cluster)
            cluster.rest = RestConnection(cluster.master)
            cluster.rest.activate_service_api(["cbas", "security"])
            # Set this unconditionally
            status, content = cluster.rest.cluster.set_internal_settings(
                "magmaMinMemoryQuota", 256)
            if not status:
                self.fail(str(content))

        # Enforce tls on nodes of all clusters
        self.enable_tls_on_nodes()
        if self.use_https:
            for server in self.servers:
                self.set_ports_for_server(server, "ssl")
            CbServer.use_https = True

        """
        KV infra to be created per cluster.
        Accepted values are -
        bkt_spec : will create KV infra based on bucket spec. bucket_spec param needs to be passed.
        default : will create a bucket named default on the cluster.
        None : no buckets will be created on cluster
        tpch : will create KV infra for tpch data
        | -> separates number of nodes per cluster.
        """
        if self.input.param("cluster_kv_infra", None):
            self.cluster_kv_infra = self.input.param("cluster_kv_infra", None).split("|")
            if len(self.cluster_kv_infra) < self.num_of_clusters:
                self.cluster_kv_infra.extend(
                    [None] * (self.num_of_clusters - len(self.cluster_kv_infra)))
        else:
            self.cluster_kv_infra = [None] * self.num_of_clusters

        # Common properties
        self.num_concurrent_queries = self.input.param('num_queries', 5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size',
                                                      100)
        self.index_fields = self.input.param('index_fields', None)
        if self.index_fields:
            self.index_fields = self.index_fields.split("-")
        self.retry_time = self.input.param("retry_time", 300)
        self.num_retries = self.input.param("num_retries", 1)

        self.cbas_spec_name = self.input.param("cbas_spec", None)
        self.columnar_spec_name = self.input.param("columnar_spec_name", None)

        self.expected_error = self.input.param("error", None)

        self.bucket_spec = self.input.param("bucket_spec", "analytics.default")
        self.doc_spec_name = self.input.param("doc_spec_name", "initial_load")

        self.set_default_cbas_memory = self.input.param(
            'set_default_cbas_memory', False)

        self.cbas_memory_quota_percent = int(self.input.param(
            "cbas_memory_quota_percent", 80))
        self.bucket_size = self.input.param("bucket_size", 256)

        self.cbas_util = CbasUtil(self.task)

        self.tpch_util = TPCHUtil(self)

        self.service_mem_dict = {
            "kv": [CbServer.Settings.KV_MEM_QUOTA,
                   CbServer.Settings.MinRAMQuota.KV, 0],
            "fts": [CbServer.Settings.FTS_MEM_QUOTA,
                    CbServer.Settings.MinRAMQuota.FTS, 0],
            "index": [CbServer.Settings.INDEX_MEM_QUOTA,
                      CbServer.Settings.MinRAMQuota.INDEX, 0],
            "cbas": [CbServer.Settings.CBAS_MEM_QUOTA,
                     CbServer.Settings.MinRAMQuota.CBAS, 0],
        }
        # Add nodes to the cluster as per node_init param.
        for i, (cluster_name, cluster) in enumerate(self.cb_clusters.items()):

            cluster_services = self.cluster_util.get_services_map(cluster)
            cluster_info = self.cluster_util.get_nodes_self(cluster.master)

            for service in cluster_services:
                if service != "n1ql":
                    property_name = self.service_mem_dict[service][0]
                    service_mem_in_cluster = cluster_info.__getattribute__(property_name)
                    self.service_mem_dict[service][2] = service_mem_in_cluster

            j = 1
            for server in cluster.servers:
                # This check ensures that only the number of nodes that are
                # mentioned in node_init param are added to the cluster.
                if j < self.nodes_init[i]:
                    if server.ip != cluster.master.ip:
                        server.services = self.services_init[i][j].replace(":", ",")
                        j += 1
                        if self.set_default_cbas_memory:
                            self.log.info(
                                "Setting the min possible memory quota so that adding "
                                "more nodes to the cluster wouldn't be a problem.")
                            cluster.rest.cluster.configure_memory(
                                {
                                    CbServer.Settings.KV_MEM_QUOTA:
                                        CbServer.Settings.MinRAMQuota.KV,
                                    CbServer.Settings.FTS_MEM_QUOTA:
                                        CbServer.Settings.MinRAMQuota.FTS,
                                    CbServer.Settings.INDEX_MEM_QUOTA:
                                        CbServer.Settings.MinRAMQuota.INDEX
                                })

                            self.log.info("Setting %d memory quota for CBAS" % CbServer.Settings.MinRAMQuota.CBAS)
                            cluster.cbas_memory_quota = CbServer.Settings.MinRAMQuota.CBAS
                            cluster.rest.cluster.configure_memory(
                                {
                                    CbServer.Settings.CBAS_MEM_QUOTA: CbServer.Settings.MinRAMQuota.CBAS
                                })
                        else:
                            self.set_memory_for_services(
                                cluster, server, server.services)

            if cluster.servers[1:]:
                self.task.rebalance(
                    cluster, cluster.servers[1:self.nodes_init[i]], [],
                    services=[
                        server.services for server in cluster.servers[
                                                      1:self.nodes_init[i]]])
                cluster.available_servers = cluster.servers[self.nodes_init[i]:]

            if cluster.cbas_nodes:
                cbas_cc_node_ip = None
                retry = 0
                while True and retry < 3:
                    cbas_cc_node_ip = self.cbas_util.retrieve_cc_ip_from_master(
                        cluster)
                    if cbas_cc_node_ip:
                        break
                    else:
                        self.sleep(10, "Waiting for CBAS service to come up")
                        retry += 1

                if not cbas_cc_node_ip:
                    self.fail("CBAS service did not come up even after 15 "
                              "mins.")

                for server in cluster.cbas_nodes:
                    if server.ip == cbas_cc_node_ip:
                        cluster.cbas_cc_node = server
                        break

            if "cbas" in cluster.master.services:
                self.cbas_util.cleanup_cbas(cluster)

            cluster.otpNodes = self.cluster_util.get_otp_nodes(cluster.master)

            if self.multiple_ca:
                cluster.x509 = x509main(
                    host=cluster.master, standard=self.standard,
                    encryption_type=self.encryption_type,
                    passphrase_type=self.passphrase_type)
                if len(self.cb_clusters) == 1:
                    servers = self.servers
                else:
                    servers = cluster.servers
                self.generate_and_upload_cert(
                    servers, cluster.x509, upload_root_certs=True,
                    upload_node_certs=True, upload_client_certs=True)
                payload = "name=cbadminbucket&roles=admin&password=password"
                cluster.rest.security.create_local_user("cbadminbucket", payload)

            # Wait for analytics service to be up.
            if hasattr(cluster, "cbas_cc_node"):
                if not self.cbas_util.is_analytics_running(cluster):
                    self.fail("Analytics service did not come up even after 10\
                                         mins of wait after initialisation")

            if self.input.param("n2n_encryption", False):

                self.security_util = SecurityUtils(self.log)
                self.log.info("Disabling Auto-Failover")
                status, content = cluster.rest.cluster.update_auto_failover_settings(
                    "false", 120)
                if not status:
                    self.log.error(str(content))
                    self.fail("Disabling Auto-Failover failed")

                self.log.info("Setting node to node encryption level to {0}".format(
                    self.input.param("n2n_encryption_level", "control")))
                self.security_util.set_n2n_encryption_level_on_nodes(
                    cluster.nodes_in_cluster,
                    level=self.input.param("n2n_encryption_level", "control"))

                CbServer.use_https = True
                self.log.info("Enabling Auto-Failover")
                status, content = cluster.rest.cluster.update_auto_failover_settings(
                    "true", 300)
                if not status:
                    self.log.error(str(content))
                    self.fail("Enabling Auto-Failover failed")

                for server in self.input.servers:
                    self.set_ports_for_server(server, "ssl")

                if not self.cbas_util.wait_for_cbas_to_recover(cluster, 300):
                    self.fail("Analytics service Failed to recover")

            if self.input.param("analytics_loggers", None):
                """
                This flag is used for setting analytics internal log levels. 
                These logs are helpful while dubugging issues as they 
                provide a deeper insight into working on CBAS service.
                This flag can be used to set one or more logger for analytics.
                logger_name_1:level-logger_name_2:level-......
                """
                cbas_loggers = self.input.param("analytics_loggers",
                                                None).split("-")
                log_level_dict = dict()
                for logger in cbas_loggers:
                    tmp = logger.split(":")
                    log_level_dict[tmp[0]] = tmp[1]
                self.log.info("Setting following log levels for analytics - "
                              "{0}".format(log_level_dict))
                status, content, response = self.cbas_util.set_log_level_on_cbas(
                    self.cluster, log_level_dict, timeout=120)
                if not status:
                    self.fail("Error while setting log level for CBAS - "
                              "{0}".format(content))

                self.log.info("Verifying whether log levels set successfully")
                status, content, response = self.cbas_util.get_log_level_on_cbas(
                    self.cluster)
                match_counter = 0
                if status:
                    actual_log_levels = content["loggers"]
                    for logger in actual_log_levels:
                        if (logger["name"] in log_level_dict) and \
                                logger["level"] == log_level_dict[logger["name"]]:
                            match_counter += 1
                    if match_counter == len(log_level_dict):
                        self.log.info("All log levels were set successfully")
                    else:
                        self.fail("Some log levels were not set")
                else:
                    self.fail("Error while fetching log levels")

            self.disk_optimized_thread_settings = self.input.param(
                "disk_optimized_thread_settings", False)
            if self.disk_optimized_thread_settings:
                self.bucket_util.update_memcached_num_threads_settings(
                    cluster.master,
                    num_writer_threads="disk_io_optimized",
                    num_reader_threads="disk_io_optimized")

            if self.cluster_kv_infra[i] == "bkt_spec":
                if self.bucket_spec is not None:
                    try:
                        self.collectionSetUp(cluster)
                    except Exception as exception:
                        self.handle_setup_exception(exception)
                else:
                    self.fail("Error : bucket_spec param needed")
            elif self.cluster_kv_infra[i] == "tpch":
                # Load tpch buckets, min KV memory required is 1800 MB
                self.tpch_util.create_kv_buckets_for_tpch(cluster)

                # Load data into tpch buckets
                self.tpch_util.load_tpch_data_into_KV_buckets(cluster)
            elif self.cluster_kv_infra[i] == "default":
                self.bucket_util.create_default_bucket(
                    cluster,
                    bucket_type=self.bucket_type,
                    ram_quota=self.bucket_size,
                    replica=self.num_replicas,
                    conflict_resolution=self.bucket_conflict_resolution_type,
                    replica_index=self.bucket_replica_index,
                    storage=self.bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    flush_enabled=self.flush_enabled)

            self.bucket_util.add_rbac_user(cluster.master)

        for cluster in sorted(self.cb_clusters.values(), key=lambda x: x.name):
            cluster.srv = None
            if hasattr(cluster, "cbas_cc_node"):
                self.analytics_cluster = cluster
            else:
                self.remote_cluster = cluster
                self.analytics_cluster = cluster
        self.sdk_clients_per_user = self.input.param("sdk_clients_per_user", 1)
        ColumnarBaseTest.init_sdk_pool_object(
            self.analytics_cluster, self.sdk_clients_per_user,
            self.analytics_cluster.master.rest_username,
            self.analytics_cluster.master.rest_password)
        self.log.info("=== CBAS_BASE setup was finished for test #{0} {1} ==="
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        if self.input.param("n2n_encryption", False):
            for i, (cluster_name, cluster) in enumerate(self.cb_clusters.items()):
                self.log.info("Disabling Auto-Failover")
                status, content = cluster.rest.cluster.update_auto_failover_settings(
                    "false", 120, fo_on_disk_issue="false")
                if not status:
                    self.log.error(str(content))
                    self.fail("Disabling Auto-Failover failed")

                self.log.info("Disabling node to node encryption")
                self.security_util.disable_n2n_encryption_cli_on_nodes(cluster.nodes_in_cluster)
                CbServer.use_https = True

                self.log.info("Enabling Auto-Failover")
                status, content = cluster.rest.cluster.update_auto_failover_settings(
                    "true", 120, fo_on_disk_issue="false")
                if not status:
                    self.fail("Enabling Auto-Failover failed")
                # Waiting for CBAS to recover so that it can be removed successfully during
                # teardown.
                if not self.cbas_util.wait_for_cbas_to_recover(cluster, 300):
                    self.fail("Analytics service Failed to recover")

        if self.disk_optimized_thread_settings:
            for cluster in self.cb_clusters.values():
                self.bucket_util.update_memcached_num_threads_settings(
                    cluster.master,
                    num_writer_threads="default",
                    num_reader_threads="default",
                    num_storage_threads="default")
        super(CBASBaseTest, self).tearDown()

    def set_memory_for_services(self, cluster, server, services):
        services = services.split(",")
        if len(services) > 0:
            if "n1ql" in services:
                services.remove("n1ql")
            # Get all services that are already running in cluster
            cluster_services = self.cluster_util.get_services_map(cluster)
            cluster_info = self.cluster_util.get_nodes_self(cluster.master)
            memory_quota_available = 0
            while memory_quota_available == 0:
                info = self.cluster_util.get_nodes_self(server)
                self.log.debug("Info from server - {0}".format(info.__dict__))
                memory_quota_available = info.mcdMemoryReserved
                if memory_quota_available:
                    break
                else:
                    self.sleep(5, "Waiting before retrying to fetch node "
                                  "stats")
            if len(services) == 1:
                service = services[0]
                if service in cluster_services:
                    if service != "kv":
                        property_name = self.service_mem_dict[service][0]
                        service_mem_in_cluster = cluster_info.__getattribute__(
                            property_name)

                        """If service is already in cluster, we cannot
                        increase the RAM allocation, but we can reduce the
                        RAM allocation if needed."""
                        if service == "cbas":
                            memory_quota_available = memory_quota_available * \
                                           self.cbas_memory_quota_percent / 100

                        """if memory_quota_available is greater than
                        service_mem_in_cluster, then we don't need to
                        reallocate memory, as the total memory available is
                        sufficient to absorb the memory that is already
                        allocated to the service"""
                        if memory_quota_available <= service_mem_in_cluster:
                            if self.service_mem_dict[service][2]:
                                if memory_quota_available <= \
                                        self.service_mem_dict[service][2]:
                                    if memory_quota_available < \
                                            self.service_mem_dict[service][1]:
                                        self.fail(
                                            "Minimum memory required for {0} "
                                            "is {1}MB, but only {2}MB is "
                                            "available".format(
                                                service,
                                                self.service_mem_dict[service][1],
                                                memory_quota_available))
                                    else:
                                        self.log.info(
                                            "Setting {0} memory quota for {1}"
                                            .format(memory_quota_available,
                                                    service))
                                        cluster.rest.cluster.configure_memory(
                                            {property_name: memory_quota_available})
                                        self.service_mem_dict[service][2] = memory_quota_available
                            elif memory_quota_available >= \
                                    self.service_mem_dict[service][1]:
                                self.log.info(
                                    "Setting {0} memory quota for {1}"
                                    .format(memory_quota_available, service))
                                cluster.rest.cluster.configure_memory(
                                    {property_name: memory_quota_available})
                                self.service_mem_dict[service][
                                    2] = memory_quota_available
                            else:
                                self.fail(
                                    "Error while setting service memory "
                                    "quota {0} for {1}".format(
                                        self.service_mem_dict[service][1],
                                        service))
                else:
                    if service == "cbas":
                        memory_quota_available = memory_quota_available * \
                                                 self.cbas_memory_quota_percent / 100
                    property_name = self.service_mem_dict[service][0]
                    self.log.info("Setting {0} memory quota for {1}".format(
                        memory_quota_available, service))
                    if memory_quota_available >= self.service_mem_dict[service][1]:
                        if self.service_mem_dict[service][2]:
                            if memory_quota_available <= self.service_mem_dict[service][2]:
                                self.log.info(
                                    "Setting {0} memory quota for {1}".format(
                                        memory_quota_available, service))
                                cluster.rest.cluster.configure_memory(
                                    {property_name: memory_quota_available})
                                self.service_mem_dict[service][2] = memory_quota_available
                        else:
                            self.log.info(
                                "Setting {0} memory quota for {1}".format(
                                    memory_quota_available, service))
                            cluster.rest.cluster.configure_memory(
                                {property_name: memory_quota_available})
                            self.service_mem_dict[service][2] = memory_quota_available
                    else:
                        self.fail(
                            "Error while setting service mem quota %s for %s"
                            % (self.service_mem_dict[service][1], service))
            else:
                """If KV is present, then don't change the KV memory quota
                Assuming that KV node will always be present in the master"""
                if "kv" in services:
                    services.remove("kv")
                    memory_quota_available -= cluster_info\
                        .__getattribute__(CbServer.Settings.KV_MEM_QUOTA)

                set_cbas_mem = False
                if "cbas" in services:
                    services.remove("cbas")
                    set_cbas_mem = True

                for service in services:
                    # setting minimum possible memory for other services.
                    if memory_quota_available >= self.service_mem_dict[service][1]:
                        if self.service_mem_dict[service][2]:
                            if memory_quota_available > self.service_mem_dict[service][2]:
                                memory_quota = self.service_mem_dict[service][2]
                            else:
                                memory_quota = memory_quota_available
                        else:
                            memory_quota = self.service_mem_dict[service][1]
                        self.log.info(
                            "Setting {0} memory quota for {1}".format(
                                memory_quota, service))
                        cluster.rest.cluster.configure_memory(
                            {self.service_mem_dict[service][0]: memory_quota})
                        self.service_mem_dict[service][2] = memory_quota
                        memory_quota_available -= memory_quota
                    else:
                        self.fail(
                            "Error while setting service mem quota %s for %s"
                            % (self.service_mem_dict[service][1], service))
                if set_cbas_mem:
                    if memory_quota_available >= self.service_mem_dict["cbas"][1]:
                        if "cbas" in cluster_services:
                            if cluster_info.__getattribute__(
                                    CbServer.Settings.CBAS_MEM_QUOTA) >= memory_quota_available:
                                self.log.info(
                                    "Setting {0} memory quota for CBAS".format(
                                        memory_quota_available))
                                cluster.rest.cluster.configure_memory(
                                    {CbServer.Settings.CBAS_MEM_QUOTA: memory_quota_available})
                        else:
                            self.log.info(
                                "Setting {0} memory quota for CBAS".format(
                                    memory_quota_available))
                            cluster.rest.cluster.configure_memory(
                                {CbServer.Settings.CBAS_MEM_QUOTA: memory_quota_available})
                        self.service_mem_dict[CbServer.Services.CBAS][2] \
                            = memory_quota_available
                    else:
                        self.fail("Error while setting service memory quota {0} "
                                  "for CBAS".format(memory_quota_available))

    def collectionSetUp(
            self, cluster, load_data=True, buckets_spec=None,
            doc_loading_spec=None, create_sdk_clients=True):
        """
        Setup the buckets, scopes and collecitons based on the spec passed.
        """
        # If True, creates bucket/scope/collections with simpler names
        self.use_simple_names = self.input.param("use_simple_names", True)
        self.remove_default_collection = self.input.param(
            "remove_default_collection", False)

        # Create bucket(s) and add rbac user
        self.bucket_util.add_rbac_user(cluster.master)
        if not buckets_spec:
            buckets_spec = self.bucket_util.get_bucket_template_from_package(
                self.bucket_spec)

        buckets_spec[MetaConstants.USE_SIMPLE_NAMES] = self.use_simple_names

        if self.bucket_storage == Bucket.StorageBackend.magma:
            buckets_spec[Bucket.storageBackend] = Bucket.StorageBackend.magma
            buckets_spec[Bucket.evictionPolicy] = Bucket.EvictionPolicy.FULL_EVICTION

            if Bucket.ramQuotaMB not in buckets_spec or \
                (Bucket.ramQuotaMB in buckets_spec and buckets_spec[Bucket.ramQuotaMB] < 256):
                buckets_spec[Bucket.ramQuotaMB] = 256

        else:
            buckets_spec[Bucket.storageBackend] = Bucket.StorageBackend.couchstore

            if Bucket.evictionPolicy not in buckets_spec:
                buckets_spec[Bucket.evictionPolicy] = Bucket.EvictionPolicy.VALUE_ONLY

            if Bucket.ramQuotaMB not in buckets_spec:
                buckets_spec[Bucket.ramQuotaMB] = 100

        self.cluster_util.update_cluster_nodes_service_list(cluster)
        # Process params to over_ride values if required
        self.over_ride_bucket_template_params(cluster, buckets_spec)
        self.bucket_util.create_buckets_using_json_data(cluster, buckets_spec)
        self.bucket_util.wait_for_collection_creation_to_complete(cluster)

        # Prints bucket stats before doc_ops
        self.bucket_util.print_bucket_stats(cluster)

        if create_sdk_clients:
            CollectionBase.create_clients_for_sdk_pool(self, cluster)
        self.cluster_util.print_cluster_stats(cluster)
        if load_data:
            self.load_data_into_buckets(cluster, doc_loading_spec)

    def load_data_into_buckets(self, cluster, doc_loading_spec=None,
                               async_load=False, validate_task=True,
                               mutation_num=0):
        """
        Loads data into buckets using the data spec
        """
        if not doc_loading_spec:
            doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                self.doc_spec_name)
        CollectionBase.over_ride_doc_loading_template_params(
            self, doc_loading_spec)
        CollectionBase.set_retry_exceptions(
            doc_loading_spec, self.durability_level)

        doc_loading_task = self.bucket_util.run_scenario_from_spec(
            self.task, cluster, cluster.buckets, doc_loading_spec,
            mutation_num=mutation_num, batch_size=self.batch_size,
            async_load=async_load, validate_task=validate_task)
        if doc_loading_task.result is False:
            self.fail("Initial reloading failed")
        ttl_buckets = [
            "multi_bucket.buckets_for_rebalance_tests_with_ttl",
            "multi_bucket.buckets_all_membase_for_rebalance_tests_with_ttl",
            "volume_templates.buckets_for_volume_tests_with_ttl"]
        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets(cluster, cluster.buckets)
        if self.bucket_spec not in ttl_buckets:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                cluster, timeout=2400)

        self.bucket_util.print_bucket_stats(self.cluster)

    def over_ride_bucket_template_params(self, cluster, bucket_spec):
        for key, val in self.input.test_params.items():
            if key == "replicas":
                bucket_spec[Bucket.replicaNumber] = self.num_replicas
            elif key == "remove_default_collection":
                bucket_spec[MetaConstants.REMOVE_DEFAULT_COLLECTION] = \
                    self.input.param(key)
            elif key == "flushEnabled":
                bucket_spec[Bucket.flushEnabled] = int(self.flush_enabled)
            elif key == "num_buckets":
                bucket_spec[MetaConstants.NUM_BUCKETS] = int(
                    self.input.param("num_buckets", 1))
            elif key == "num_scopes":
                bucket_spec[MetaConstants.NUM_SCOPES_PER_BUCKET] = int(
                    self.input.param("num_scopes", 1))
            elif key == "num_collections":
                bucket_spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE] = int(
                    self.input.param("num_collections", 1))
            elif key == "num_items":
                bucket_spec[MetaConstants.NUM_ITEMS_PER_COLLECTION] = \
                    self.num_items
            elif key == "bucket_type":
                bucket_spec[Bucket.bucketType] = self.bucket_type
            elif key == "bucket_eviction_policy":
                bucket_spec[Bucket.evictionPolicy] = self.bucket_eviction_policy
            elif key == "bucket_storage":
                bucket_spec[Bucket.storageBackend] = self.bucket_storage
            elif key == "compression_mode":
                bucket_spec[Bucket.compressionMode] = self.compression_mode
            elif key == "bucket_history_retention_seconds":
                bucket_spec[Bucket.historyRetentionSeconds] = int(self.bucket_dedup_retention_seconds)
            elif key == "bucket_history_retention_bytes":
                bucket_spec[Bucket.historyRetentionBytes] = int(self.bucket_dedup_retention_bytes)
        if "bucket_size" in self.input.test_params:
            if self.bucket_size == "auto":
                cluster_info = self.cluster_util.get_nodes_self(cluster.master)
                kv_quota = cluster_info.__getattribute__(CbServer.Settings.KV_MEM_QUOTA)
                self.bucket_size = int((kv_quota // bucket_spec[
                    MetaConstants.NUM_BUCKETS]) * 0.9)
            bucket_spec[Bucket.ramQuotaMB] = self.bucket_size

    @staticmethod
    def create_or_delete_users(rbac_util, rbac_users_created, delete=False):
        """
        Creates all types of rbac users.
        """
        if delete:
            for user in rbac_users_created:
                try:
                    rbac_util._drop_user(user)
                    del (rbac_users_created[user])
                except:
                    pass
        else:
            for role in rbac_util.cb_server_roles:
                if "[*]" in role:
                    user = role.replace("[*]", "")
                else:
                    user = role
                rbac_users_created[user] = role
                rbac_util._create_user_and_grant_role(user, role)

    @staticmethod
    def create_testcase_for_rbac_user(description, rbac_users_created,
                                      users_with_permission=[],
                                      users_without_permission=[]):
        testcases = []
        for user in rbac_users_created:
            if user in ["admin", "analytics_admin"] + users_with_permission:
                test_params = {
                    "description": description.format(user),
                    "validate_error_msg": False
                }
            elif user in ["security_admin_local", "security_admin_external",
                          "query_external_access",
                          "query_system_catalog", "replication_admin",
                          "ro_admin", "bucket_full_access",
                          "replication_target", "mobile_sync_gateway",
                          "data_reader", "data_writer",
                          "data_dcp_reader", "data_monitoring",
                          "views_admin", "views_reader",
                          "query_delete", "query_insert",
                          "query_manage_index", "query_select",
                          "query_update", "fts_admin", "fts_searcher",
                          "cluster_admin", "bucket_admin",
                          "analytics_manager", "data_backup", "analytics_select",
                          "analytics_reader"] + users_without_permission:
                test_params = {
                    "description": description.format(user),
                    "validate_error_msg": True,
                    "expected_error": "User must have permission",
                }
            else:
                test_params = {"description": description.format(user),
                               "validate_error_msg": True,
                               "expected_error": "Unauthorized user"}
            test_params["username"] = user
            testcases.append(test_params)
        return testcases

    def setup_for_test(self, update_spec={}, wait_for_ingestion=True):
        if not update_spec:
            update_spec = {
                "no_of_dataverses": self.input.param('no_of_dv', 1),
                "no_of_datasets_per_dataverse": self.input.param('ds_per_dv',
                                                                 1),
                "no_of_synonyms": self.input.param('no_of_syn', 0),
                "no_of_indexes": self.input.param('no_of_idx', 1),
                "max_thread_count": self.input.param('no_of_threads', 1),
                "dataset": {
                    "creation_methods": ["cbas_collection", "cbas_dataset"]}}
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util.get_cbas_spec(
                self.cbas_spec_name)
            if update_spec:
                self.cbas_util.update_cbas_spec(self.cbas_spec, update_spec)
            cbas_infra_result = self.cbas_util.create_cbas_infra_from_spec(
                self.cluster, self.cbas_spec, self.bucket_util,
                wait_for_ingestion=wait_for_ingestion)
            if not cbas_infra_result[0]:
                self.fail(
                    "Error while creating infra from CBAS spec -- " +
                    cbas_infra_result[1])
        self.replica_num = self.input.param('replica_num', 0)
        if self.replica_num:
            set_result = self.cbas_util.set_replica_number_from_settings(
                self.cluster.master, replica_num=self.replica_num)
            if set_result != self.replica_num:
                self.fail("Error while setting replica for CBAS")

            self.log.info(
                "Rebalancing for CBAS replica setting change to take "
                "effect.")
            rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
                cbas_nodes_out=0, available_servers=self.available_servers,
                exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance failed")
