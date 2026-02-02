import json
import time
from datetime import datetime
import os
import re
import socket
import traceback
import random

from ruamel.yaml import YAML

import global_vars
from BucketLib.bucket import Bucket
from cb_constants import ClusterRun, CbServer
import cb_constants
from SystemEventLogLib.Events import Event
from SystemEventLogLib.data_service_events import DataServiceEvents
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import BucketUtils
from constants.platform_constants import os_constants
from cb_basetest import CouchbaseBaseTest
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster,\
    Nebula
from couchbase_utils.security_utils.security_utils import SecurityUtils
from couchbase_utils.security_utils.x509_multiple_CA_util import x509main
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClientPool, SDKClient
from security_config import trust_all_certs
from docker_utils.DockerSDK import DockerClient
from awsLib.s3_data_helper import perform_S3_operation


class OnPremBaseTest(CouchbaseBaseTest):
    def setUp(self):
        super(OnPremBaseTest, self).setUp()

        # Framework specific parameters (Extension from cb_basetest)
        self.skip_cluster_reset = self.input.param("skip_cluster_reset", False)
        self.skip_setup_cleanup = self.input.param("skip_setup_cleanup", False)
        # End of framework parameters

        # Cluster level info settings
        self.log_info = self.input.param("log_info", None)
        self.minimum_bucket_replica = self.input.param("minimum_bucket_replica", None)
        self.disable_max_fo_count = self.input.param("disable_max_fo_count",
                                                     'false')
        self.bypass_encryption_restrictions = self.input.param("bypass_encryption_restrictions", True)
        self.log_location = self.input.param("log_location", None)
        self.stat_info = self.input.param("stat_info", None)
        self.port = self.input.param("port", None)
        self.port_info = self.input.param("port_info", None)
        self.servers = self.input.servers
        self.spare_nodes = list()
        self.num_servers = self.input.param("servers", len(self.servers))
        self.server_groups = self.input.param("server_groups",
                                              CbServer.default_server_group)
        self.vbuckets = self.input.param("vbuckets", None)
        self.gsi_type = self.input.param("gsi_type", 'plasma')
        # Memory quota settings
        # Max memory quota to utilize per node
        self.quota_percent = self.input.param("quota_percent", 100)
        # Services' RAM quota to set on cluster
        self.kv_mem_quota_percent = self.input.param("kv_quota_percent", None)
        self.index_mem_quota_percent = \
            self.input.param("index_quota_percent", None)
        self.fts_mem_quota_percent = \
            self.input.param("fts_quota_percent", None)
        self.cbas_mem_quota_percent = \
            self.input.param("cbas_quota_percent", None)
        self.eventing_mem_quota_percent = \
            self.input.param("eventing_quota_percent", None)
        # CBAS setting
        self.jre_path = self.input.param("jre_path", None)
        self.enable_dp = self.input.param("enable_dp", False)
        # End of cluster info parameters

        self.bucket_replica_index = self.input.param("bucket_replica_index",
                                                     1)
        self.bucket_num_vb = self.input.param("bucket_num_vb", None)
        # End of bucket parameters

        self.services_in = self.input.param("services_in", None)
        self.forceEject = self.input.param("forceEject", False)
        self.wait_timeout = self.input.param("wait_timeout", 120)
        self.verify_unacked_bytes = \
            self.input.param("verify_unacked_bytes", False)
        self.disabled_consistent_view = \
            self.input.param("disabled_consistent_view", None)
        self.rebalanceIndexWaitingDisabled = \
            self.input.param("rebalanceIndexWaitingDisabled", None)
        self.rebalanceIndexPausingDisabled = \
            self.input.param("rebalanceIndexPausingDisabled", None)
        self.maxParallelIndexers = \
            self.input.param("maxParallelIndexers", None)
        self.maxParallelReplicaIndexers = \
            self.input.param("maxParallelReplicaIndexers", None)
        self.use_https = self.input.param("use_https", True)
        self.enforce_tls = self.input.param("enforce_tls", True)
        self.encryption_level = self.input.param("encryption_level", "all")
        self.private_key_passphrase = self.input.param("private_key_passphrase", None)
        self.kmip_key_uuid = self.input.param("kmip_key_uuid", None)
        config_path = os.path.join(os.path.dirname(__file__), "kmip_config.json")
        with open(config_path, "r") as f:
            kmip_config = json.load(f)
        self.kmip_ip = kmip_config["kmip_ip"]
        self.kmip_host_name = kmip_config["host_name"]
        self.kmip_password = self.input.param("kmip_password", None)
        self.kmip_cert_path = kmip_config["certs"]["path"]
        self.KMIP_pkcs8_file_name = self.input.param(
            "KMIP_pkcs8_file_name", "client-key-pkcs8.pem")
        self.KMIP_cert_file_name = self.input.param(
            "KMIP_cert_file_name", "client_cert_with_appid2.pem")
        self.client_certs_path = "/etc/couchbase/certs/"
        self.create_KMIP_secret= self.input.param(
            "create_KMIP_secret", False)
        self.KMIP_for_log_encryption = self.input.param(
            "KMIP_for_log_encryption", False)
        self.KMIP_for_config_encryption = self.input.param(
            "KMIP_for_config_encryption", False)
        self.KMIP_for_data_encryption = self.input.param(
            "KMIP_for_data_encryption", False)
        self.KMIP_for_audit_encryption = self.input.param(
            "KMIP_for_audit_encryption", False)
        self.enable_encryption_at_rest = self.input.param(
            "enable_encryption_at_rest", False)
        self.encryption_at_rest_id = self.input.param(
            "encryption_at_rest_id", None)
        self.KMIP_id = self.input.param("KMIP_id", None)
        self.enable_config_encryption_at_rest = self.input.param(
            "enable_config_encryption_at_rest", False)
        self.config_encryption_at_rest_id = self.input.param(
            "config_encryption_at_rest_id", None)
        self.enable_log_encryption_at_rest = self.input.param(
            "enable_log_encryption_at_rest", False)
        self.enable_audit_encryption_at_rest = self.input.param(
            "enable_audit_encryption_at_rest", False)
        self.audit_encryption_at_rest_id = self.input.param(
            "audit_encryption_at_rest_id", False)
        self.log_encryption_at_rest_id = self.input.param(
            "log_encryption_at_rest_id", False)
        self.secret_rotation_interval = self.input.param(
            "secret_rotation_interval", 60)
        self.config_dekLifetime = self.input.param(
            "config_dekLifetime", CbServer.secret_rotation_interval_in_seconds)
        self.log_dekLifetime = self.input.param(
            "log_dekLifetime", CbServer.encryption_at_rest_dek_lifetime_interval)
        self.config_dekRotationInterval = self.input.param(
            "config_dekRotationInterval", CbServer.encryption_at_rest_dek_rotation_interval)
        self.audit_dekRotationInterval = self.input.param(
            "audit_dekRotationInterval",
            CbServer.encryption_at_rest_dek_rotation_interval)
        self.audit_dekLifetime = self.input.param(
            "audit_dekLifetime",
            CbServer.encryption_at_rest_dek_lifetime_interval)
        self.log_dekRotationInterval = self.input.param(
            "log_dekRotationInterval", CbServer.encryption_at_rest_dek_rotation_interval)
        self.secret_id = self.input.param("secret_id", None)
        self.encryptionAtRestDekRotationInterval = self.input.param(
            "encryptionAtRestDekRotationInterval", CbServer.encryption_at_rest_dek_rotation_interval)
        self.rotationIntervalInSeconds = self.input.param(
            "rotationIntervalInSeconds", CbServer.secret_rotation_interval_in_seconds)
        self.encryption_at_rest_dek_lifetime = self.input.param(
            "encryption_at_rest_dek_lifetime", CbServer.encryption_at_rest_dek_lifetime_interval)
        self.ipv4_only = self.input.param("ipv4_only", False)
        self.ipv6_only = self.input.param("ipv6_only", False)
        self.multiple_ca = self.input.param("multiple_ca", False)
        # User defined throttling limit used in serverless config
        self.kv_throttling_limit = \
            self.input.param("kv_throttling_limit", 999999)
        self.kv_storage_limit = \
            self.input.param("kv_storage_limit", 100000)

        # To enable analytics compute storage separation
        self.analytics_compute_storage_separation = self.input.param(
            "analytics_compute_storage_separation", False)

        self.node_utils.cleanup_pcaps(self.servers)
        self.collect_pcaps = self.input.param("collect_pcaps", False)
        if self.collect_pcaps:
            self.node_utils.start_collect_pcaps(self.servers)

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

        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)
        global_vars.cluster_util = self.cluster_util
        global_vars.bucket_util = self.bucket_util

        # Sets internal password rotation time interval
        self.int_pwd_rotn = self.input.param("int_pwd_rotn", 1800000)

        # Populate memcached_port in case of cluster_run
        if int(self.input.servers[0].port) == ClusterRun.port:
            # This flag will be used globally to decide port_mapping stuff
            ClusterRun.is_enabled = True

        self.log_setup_status(self.__class__.__name__, "started")

        # Force disable TLS to avoid initial connection issues
        tasks = [self.node_utils.async_disable_tls(server)
                 for server in self.servers]
        for task in tasks:
            self.task_manager.get_task_result(task)

        cluster_name_format = "C%s"
        default_cluster_index = counter_index = 1
        num_vb = self.vbuckets or CbServer.total_vbuckets
        if len(self.input.clusters) > 1:
            # Multi cluster setup
            for _, nodes in self.input.clusters.iteritems():
                cluster_name = cluster_name_format % counter_index
                tem_cluster = CBCluster(name=cluster_name, servers=nodes,
                                        vbuckets=num_vb)
                self.cb_clusters[cluster_name] = tem_cluster
                counter_index += 1
        else:
            # Single cluster
            cluster_name = cluster_name_format % counter_index
            self.cb_clusters[cluster_name] = CBCluster(name=cluster_name,
                                                       servers=self.servers,
                                                       vbuckets=num_vb)

        # Fetch the profile_type from the master node
        # Value will be default / serverless / columnar
        CbServer.cluster_profile = \
            self.cluster_util.get_server_profile_type(self.servers)

        # Enable use_https and enforce_tls for 'serverless' cluster testing
        # And set default bucket/cluster setting values to tests
        if CbServer.cluster_profile == "serverless":
            self.use_https = True
            self.enforce_tls = True
            self.encryption_level = "strict"

            self.bucket_storage = Bucket.StorageBackend.magma
            self.num_replicas = Bucket.ReplicaNum.TWO
            self.server_groups = "test_zone_1:test_zone_2:test_zone_3"

        if self.use_https:
            CbServer.use_https = True
            trust_all_certs()

        # Initialize self.cluster with first available cluster as default
        self.cluster = self.cb_clusters[cluster_name_format
                                        % default_cluster_index]
        CbServer.enterprise_edition = \
            self.cluster_util.is_enterprise_edition(self.cluster)
        self.cluster.edition = "enterprise" \
            if CbServer.enterprise_edition else "community"

        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.cluster.master,
                                                self.standard_buckets)

        for cluster_name, cluster in self.cb_clusters.items():
            # Append initial master node to the nodes_in_cluster list
            cluster.nodes_in_cluster.append(cluster.master)

            shell = RemoteMachineShellConnection(cluster.master)
            self.os_info = shell.extract_remote_info().type.lower()
            if self.os_info != 'windows':
                if cluster.master.ssh_username != "root":
                    self.nonroot = True
                    shell.disconnect()
                    break
            shell.disconnect()

            # SDKClientPool object for creating generic clients across tasks
            if self.sdk_client_pool is True:
                cluster.sdk_client_pool = SDKClientPool()

        self.log_setup_status("OnPremBaseTest", "started")
        try:
            # Construct dict of mem. quota percent / mb per service
            mem_quota_percent = dict()
            # Construct dict of mem. quota percent per service
            if self.kv_mem_quota_percent:
                mem_quota_percent[CbServer.Services.KV] = \
                    self.kv_mem_quota_percent
            if self.index_mem_quota_percent:
                mem_quota_percent[CbServer.Services.INDEX] = \
                    self.index_mem_quota_percent
            if self.cbas_mem_quota_percent:
                mem_quota_percent[CbServer.Services.CBAS] = \
                    self.cbas_mem_quota_percent
            if self.fts_mem_quota_percent:
                mem_quota_percent[CbServer.Services.FTS] = \
                    self.fts_mem_quota_percent
            if self.eventing_mem_quota_percent:
                mem_quota_percent[CbServer.Services.EVENTING] = \
                    self.eventing_mem_quota_percent

            if not mem_quota_percent:
                mem_quota_percent = None

            # Rotate the internal user's password at the specified interval
            self.security_util = SecurityUtils(self.log)
            self.security_util.set_internal_creds_rotation_interval(self.cluster, self.int_pwd_rotn)

            for _, cluster in self.cb_clusters.items():
                if self.use_https:
                    SDKClient.enable_tls_in_env(cluster)
                # Building this object here once based on 'use_https',
                # this will be possibly built once across the run.
                # This will minimize the mem intensity of the run drastically
                SDKClient.singleton_env_build(cluster)

            if self.skip_setup_cleanup:
                for server in self.servers:
                    self.set_ports_for_server(server, "ssl")
                # Update current server/service map and buckets for the cluster
                for _, cluster in self.cb_clusters.items():
                    self.cluster_util.update_cluster_nodes_service_list(
                        cluster)
                    cluster.buckets = self.bucket_util.get_all_buckets(cluster)
                    cluster.nodes_in_cluster = list(set(
                        cluster.kv_nodes + cluster.fts_nodes +
                        cluster.cbas_nodes + cluster.index_nodes +
                        cluster.query_nodes + cluster.eventing_nodes +
                        cluster.backup_nodes))
                return
            else:
                for cluster_name, cluster in self.cb_clusters.items():
                    self.log.info("Delete all buckets and rebalance out "
                                  "other nodes from '%s'" % cluster_name)
                    self.cluster_util.cluster_cleanup(cluster,
                                                      self.bucket_util)

            # avoid clean up if the previous test has been tear down
            if self.case_number == 1 or self.case_number > 1000:
                if self.case_number > 1000:
                    self.log.warn("TearDown for prev test failed. Will retry")
                    self.case_number -= 1000
                self.tearDownEverything(reset_cluster_env_vars=False)

            self.nebula = self.input.param("nebula", False)
            self.nebula_details = dict()
            for cluster_name, cluster in self.cb_clusters.items():
                # Check if the master node is initialized. If not, force reset
                # the cluster to avoid initial rebalance failure
                result = RestConnection(cluster.master).get_pools_default()
                if result == "unknown pool":
                    self.skip_cluster_reset = False
                if not self.skip_cluster_reset:
                    services = None
                    if self.services_init:
                        services = str(self.services_init.split("-")[0]) \
                            .replace(":", ",")
                    self.initialize_cluster(
                        cluster_name, cluster, services=services,
                        services_mem_quota_percent=mem_quota_percent)

                # Set this unconditionally
                rest_conn = RestConnection(cluster.master)
                rest_conn.set_internalSetting("magmaMinMemoryQuota", 256)
                # To set index_storage_mode = Standard Global Secondary
                rest_conn.set_indexer_storage_mode("plasma")
                self.docker_containers = []
                self.docker = None
                self.image_id = None
                if self.nebula and CbServer.cluster_profile == "serverless":
                    try:
                        # Check out nebula git repo
                        # Config nebula
                        self.log.info("launch docker container running nebula")
                        nebula_ip = self.get_executor_ip()
                        nebula_path = self.input.param("nebula_path",
                                                       "/root/direct-nebula")
                        self.create_nebula_config(nebula_path=nebula_path,
                                                  nebula_ip=nebula_ip,
                                                  cluster=cluster)
                        self.log.info("Create docker SDK client")
                        self.docker = DockerClient()
                        self.log.info("Build docker image")
                        _rand = random.randint(1, 1000)
                        self.image_id = \
                            self.docker.buildImage(nebula_path,
                                                   "directnebula-%s" % _rand)
                        self.log.info("Construct docker port mapping")
                        portMap = dict()
                        for i in range(9000, 9011):
                            i = str(i)
                            portMap.update({i: i})
                        for i in range(11000, 11011):
                            i = str(i)
                            portMap.update({i: i})
                        host_config = self.docker.portMapping(portMap)
                        self.log.info("Start Docker Container")
                        dn_container_id = self.docker.startDockerContainer(
                            host_config, self.image_id,
                            exposedPorts=portMap.keys(),
                            tag="DirectNebula-{}".format(_rand))
                        self.docker_containers.append(dn_container_id)
                        self.use_https = False
                        self.enforce_tls = False
                        CbServer.use_https = False
                        nebula = Nebula(nebula_ip, cluster.master)
                        nebula.endpoint.srv = nebula_ip
                        self.log.info("Populate Nebula object done!!")
                        self.nebula_details[cluster] = nebula
                    except:
                        traceback.print_exc()
                        self.nebula = False

            # Enable dp_version
            if self.enable_dp:
                tasks = [self.node_utils.async_enable_dp(server)
                         for server in self.cluster.servers]
                for task in tasks:
                    self.task_manager.get_task_result(task)

            # Enforce tls on nodes of all clusters
            self.enable_tls_on_nodes()


            # Creating encryption keys
            encryption_result = self.cluster_util.setup_encryption_at_rest(
                cluster_master=self.cluster.master,
                bypass_encryption_func=self.bypass_encryption_setting,
                create_KMIP_secret=self.create_KMIP_secret,
                enable_encryption_at_rest=self.enable_encryption_at_rest,
                enable_config_encryption_at_rest=self.enable_config_encryption_at_rest,
                enable_log_encryption_at_rest=self.enable_log_encryption_at_rest,
                enable_audit_encryption_at_rest=self.enable_audit_encryption_at_rest,
                secret_rotation_interval=self.secret_rotation_interval,
                kmip_key_uuid=self.kmip_key_uuid,
                client_certs_path=self.client_certs_path,
                KMIP_pkcs8_file_name=self.KMIP_pkcs8_file_name,
                KMIP_cert_file_name=self.KMIP_cert_file_name,
                private_key_passphrase=self.private_key_passphrase,
                kmip_host_name=self.kmip_host_name,
                KMIP_for_config_encryption=self.KMIP_for_config_encryption,
                config_dekLifetime=self.config_dekLifetime,
                config_dekRotationInterval=self.config_dekRotationInterval,
                KMIP_for_log_encryption=self.KMIP_for_log_encryption,
                log_dekLifetime=self.log_dekLifetime,
                log_dekRotationInterval=self.log_dekRotationInterval,
                KMIP_for_audit_encryption=self.KMIP_for_audit_encryption,
                audit_dekLifetime=self.audit_dekLifetime,
                audit_dekRotationInterval=self.audit_dekRotationInterval
            )

            # Set the returned IDs back to self
            self.cluster_util.set_encryption_ids(self,encryption_result)

            if self.use_https:
                if ClusterRun.is_enabled:
                    for index, server in enumerate(self.input.servers):
                        server.port = ClusterRun.port + (1 * index)
                        # If undefined in node.ini under 'memcached_port'
                        if server.memcached_port is CbServer.memcached_port:
                            server.memcached_port = \
                                ClusterRun.memcached_port + (2 * index)
                        if self.use_https:
                            server.port = int(server.port) + 10000
                            server.memcached_port = \
                                ClusterRun.ssl_memcached_port - (4 * index)
                else:
                    for server in self.input.servers:
                        self.set_ports_for_server(server, "ssl")
            reload(cb_constants)

            # Update initial service map for the master node
            self.cluster_util.update_cluster_nodes_service_list(self.cluster)

            # Enforce IPv4 or IPv6 or both
            if self.ipv4_only or self.ipv6_only:
                for _, cluster in self.cb_clusters.items():
                    status, msg = self.cluster_util.enable_disable_ip_address_family_type(
                        cluster, True, self.ipv4_only, self.ipv6_only)
                    if not status:
                        self.fail(msg)

            self.standard = self.input.param("standard", "pkcs8")
            self.passphrase_type = \
                self.input.param("passphrase_type", "script")
            self.encryption_type = \
                self.input.param("encryption_type", "aes256")

            for cluster_name, cluster in self.cb_clusters.items():
                self.modify_cluster_settings(cluster)

            # Track test start time only if we need system log validation
            if self.validate_system_event_logs:
                self.system_events.set_test_start_time()
            self.log_setup_status("OnPremBaseTest", "finished")
            self.__log("started")
        except Exception as e:
            traceback.print_exc()
            for server in self.input.servers:
                self.set_ports_for_server(server, "non_ssl")
            self.task.shutdown(force=True)
            self.fail(e)
        finally:
            # Track test start time only if we need system log validation
            if self.validate_system_event_logs:
                self.system_events.set_test_start_time()

            self.log_setup_status("OnPremBaseTest", "finished")

    def initialize_cluster(self, cluster_name, cluster, services=None,
                           services_mem_quota_percent=None):
        self.node_utils.reset_cluster_nodes(self.cluster_util, cluster)
        self.cluster_util.wait_for_ns_servers_or_assert(cluster.servers)
        self.sleep(5, "Wait for nodes to become ready after reset")

        self.log.info("Initializing cluster : {0}".format(cluster_name))
        # This check is to set up compute storage separation for
        # analytics in serverless mode
        self.aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", None)
        self.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", None)
        self.aws_session_token = os.getenv("AWS_SESSION_TOKEN", None)
        self.aws_bucket_region = self.input.param("aws_bucket_region", None)
        self.aws_bucket_created = False
        if (self.analytics_compute_storage_separation and
                CbServer.cluster_profile == "columnar"):
            for i in range(5):
                try:
                    self.aws_bucket_name = "columnar-build-sanity-" + str(int(
                        time.time()))
                    self.log.info("Creating S3 bucket")
                    self.aws_bucket_created = perform_S3_operation(
                        aws_access_key=self.aws_access_key,
                        aws_secret_key=self.aws_secret_key,
                        aws_session_token=self.aws_session_token,
                        create_bucket=True, bucket_name=self.aws_bucket_name,
                        region=self.aws_bucket_region)
                    break
                except Exception as e:
                    self.log.error(
                        "Creating S3 bucket - {0} in region {1}. "
                        "Failed.".format(
                            self.aws_bucket_name, self.aws_bucket_region))
                    self.log.error(str(e))
            if not self.aws_bucket_created:
                self.fail("Unable to create S3 bucket.")
            self.log.info("Adding aws bucket credentials to analytics")
            rest = RestConnection(self.cluster.master)
            status = rest.configure_compute_storage_separation_for_analytics(
                self.aws_access_key, self.aws_secret_key,
                self.aws_bucket_name, self.aws_bucket_region)
            if not status:
                self.fail("Failed to put aws credentials to analytics, "
                          "request error")

        if not services:
            master_services = self.cluster_util.get_services(
                cluster.servers[:1], self.services_init, start_node=0)
        else:
            master_services = self.cluster_util.get_services(
                cluster.servers[:1], services, start_node=0)
        if master_services is not None:
            master_services = master_services[0].split(",")

        self._initialize_nodes(
            self.task,
            cluster,
            self.disabled_consistent_view,
            self.rebalanceIndexWaitingDisabled,
            self.rebalanceIndexPausingDisabled,
            self.maxParallelIndexers,
            self.maxParallelReplicaIndexers,
            self.port,
            self.quota_percent,
            services=master_services,
            services_mem_quota_percent=services_mem_quota_percent)

        self.cluster_util.change_env_variables(cluster)
        self.cluster_util.change_checkpoint_params(cluster)
        self.log.info("Cluster %s initialized" % cluster_name)

    def modify_cluster_settings(self, cluster):
        if self.log_info:
            self.cluster_util.change_log_info(cluster, self.log_info)
        if self.log_location:
            self.cluster_util.change_log_location(cluster, self.log_location)
        if self.stat_info:
            self.cluster_util.change_stat_info(cluster, self.stat_info)
        if self.port_info:
            self.cluster_util.change_port_info(cluster, self.port_info)
        if self.port:
            self.port = self.port
        if self.minimum_bucket_replica is not None:
            rest = RestConnection(cluster.master)
            status, content = rest.set_minimum_bucket_replica_for_cluster(
                self.minimum_bucket_replica)
            self.assertTrue(status, "minimum replica setting failed to update")
            self.num_replicas = self.minimum_bucket_replica

    def start_fetch_pcaps(self, is_test_failed):
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        self.node_utils.start_fetch_pcaps(self.servers, log_path,
                                          is_test_failed)

    def set_ports_for_server(self, server, port_type="non_ssl"):
        self.log.debug("Setting %s ports for server %s" % (port_type, server))
        if port_type == "non_ssl":
            server.port = CbServer.port
            server.memcached_port = CbServer.memcached_port
            server.fts_port = CbServer.fts_port
            server.index_port = CbServer.index_port
            server.n1ql_port = CbServer.n1ql_port
            server.cbas_port = CbServer.cbas_port
            server.eventing_port = CbServer.eventing_port
        elif port_type == "ssl":
            server.port = CbServer.ssl_port
            server.memcached_port = CbServer.ssl_memcached_port
            server.fts_port = CbServer.ssl_fts_port
            server.index_port = CbServer.ssl_index_port
            server.n1ql_port = CbServer.ssl_n1ql_port
            server.cbas_port = CbServer.ssl_cbas_port
            server.eventing_port = CbServer.ssl_eventing_port

    def enable_tls_on_nodes(self):
        if self.enforce_tls:
            retry_count = self.input.param("tls_retry_count", 3)
            CbServer.n2n_encryption = True
            if self.use_https:
                CbServer.use_https = True
            retry = 0
            status = False
            while retry < retry_count:
                for _, cluster in self.cb_clusters.items():
                    status = True
                    task = self.node_utils.async_enable_tls(cluster.master,
                                                            self.encryption_level)
                    self.task_manager.get_task_result(task)
                    self.log.info("Validating if services obey tls only "
                                  "on servers {0}".format(cluster.servers))
                    if CbServer.cluster_profile != "serverless":
                        if ClusterUtils.get_encryption_level_on_node(
                                self.cluster.master) != self.encryption_level:
                            status = False
                    if self.encryption_level == "strict":
                        self.sleep(120, "waiting after enabling TLS")
                        status = self.cluster_util.check_if_services_obey_tls(
                            cluster.servers)
                if status:
                    break
                else:
                    retry += 1
                    self.sleep(10, "Retrying enforcing TLS on servers")
            else:
                self.fail("Services did not honor enforce tls")

    def tearDown(self):
        if hasattr(self, "docker_containers"):
            for container in self.docker_containers:
                self.docker.killContainer(container)
                self.log.info("Container {} killed/removed".format(container))

        if hasattr(self, "docker"):
            if self.docker is not None:
                if self.image_id is not None:
                    self.docker.deleteImage(self.image_id)
                self.docker.close()
        # Perform system event log validation and get failures (if any)
        sys_event_validation_failure = None
        if self.validate_system_event_logs:
            sys_event_validation_failure = \
                self.system_events.validate(self.cluster.master)

        if self.enable_encryption_at_rest:
            rest = RestConnection(self.cluster.master)
            for bucket in self.cluster.buckets:
                issues = rest.validate_for_encryption_at_rest_issues(bucket)
                if issues is None:
                    self.log.info("unable to reterive info from the "
                                  "bucket ".format(bucket))
                elif len(issues) > 0:
                    self.fail("Issues found in bucket related to encryption at rest {0} : {1}".format(bucket, issues))

        if self.ipv4_only or self.ipv6_only:
            for _, cluster in self.cb_clusters.items():
                self.cluster_util.enable_disable_ip_address_family_type(
                    cluster, False, self.ipv4_only, self.ipv6_only)

        # Disable n2n encryption on nodes of all clusters
        if self.use_https:
            if self.enforce_tls:
                for _, cluster in self.cb_clusters.items():
                    task = self.node_utils.async_disable_tls(cluster.master)
                    self.task_manager.get_task_result(task)
            # Set CbServer.use_https to False when
            # n2n encryption level is not strict
            CbServer.use_https = False
            CbServer.n2n_encryption = False
            if ClusterRun.is_enabled:
                for index, server in enumerate(self.input.servers):
                    server.port = server.port - 10000
                    server.memcached_port = \
                        ClusterRun.memcached_port + (2 * index)
            else:
                for server in self.input.servers:
                    self.set_ports_for_server(server, "non_ssl")

        for _, cluster in self.cb_clusters.items():
            if self.multiple_ca:
                CbServer.use_https = False
                rest = RestConnection(cluster.master)
                rest.delete_builtin_user("cbadminbucket")
                x509 = x509main(host=cluster.master)
                x509.teardown_certs(servers=cluster.servers)

            if cluster.sdk_client_pool:
                cluster.sdk_client_pool.shutdown()

        # delete aws bucket that was created for compute storage separation
        if (self.analytics_compute_storage_separation and
                CbServer.cluster_profile == "columnar"
                and self.aws_bucket_created):
            for cluster_name, cluster in self.cb_clusters.items():
                self.log.info("Resetting cluster nodes")
                self.node_utils.reset_cluster_nodes(self.cluster_util,
                                                    cluster)
            self.log.info("Deleting AWS S3 bucket - {}".format(
                self.aws_bucket_name))
            if not perform_S3_operation(
                    aws_access_key=self.aws_access_key,
                    aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token,
                    delete_bucket=True,
                    bucket_name=self.aws_bucket_name,
                    region=self.aws_bucket_region):
                self.log.error("AWS bucket failed to delete")

        result = self.check_coredump_exist(self.servers, force_collect=True)
        if self.skip_teardown_cleanup:
            self.log.debug("Skipping tearDownEverything")
        else:
            self.tearDownEverything()
        if not self.crash_warning:
            self.assertFalse(result, msg="Cb_log file validation failed")
        if self.crash_warning and result:
            self.log.warn("CRASH | CRITICAL | WARN messages found in cb_logs")

        if self.collect_pcaps:
            self.log.info("Starting Pcaps collection!!")
            self.start_fetch_pcaps(is_test_failed=self.is_test_failed())

        # Fail test in case of sys_event_logging failure
        if (not self.is_test_failed()) and sys_event_validation_failure:
            self.fail(sys_event_validation_failure)
        elif sys_event_validation_failure:
            self.log.critical("System event log validation failed: %s"
                              % sys_event_validation_failure)

        self.shutdown_task_manager()

    def tearDownEverything(self, reset_cluster_env_vars=True):
        for _, cluster in self.cb_clusters.items():
            try:
                test_failed = self.is_test_failed()
                if test_failed:
                    # Collect logs because we have not shut things down
                    if self.get_cbcollect_info:
                        self.fetch_cb_collect_logs()
                    get_trace = \
                        TestInputSingleton.input.param("get_trace", None)
                    if get_trace:
                        tasks = [
                            self.node_utils.async_get_trace(server, get_trace)
                            for server in cluster.servers]
                        for task in tasks:
                            self.task_manager.get_task_result(task)
                    else:
                        self.log.critical("Skipping get_trace !!")

                rest = RestConnection(cluster.master)
                alerts = rest.get_alerts()
                if alerts:
                    self.log.warn("Alerts found: {0}".format(alerts))
            except BaseException as e:
                # kill memcached
                traceback.print_exc()
                self.log.warning("Killing memcached due to {0}".format(e))
                self.cluster_util.kill_memcached(cluster)
                # Increase case_number to retry tearDown in setup for next test
                self.case_number += 1000
            finally:
                if reset_cluster_env_vars:
                    self.cluster_util.reset_env_variables(cluster)
        self.infra_log.info("========== tasks in thread pool ==========")
        self.task_manager.print_tasks_in_pool()
        self.infra_log.info("==========================================")

    def __log(self, status):
        try:
            msg = "{0}: {1} {2}" \
                .format(datetime.now(), self._testMethodName, status)
            RestConnection(self.servers[0]).log_client_error(msg)
        except Exception as e:
            self.log.warning("Exception during REST log_client_error: %s" % e)

    def _initialize_nodes(self, task, cluster, disabled_consistent_view=None,
                          rebalance_index_waiting_disabled=None,
                          rebalance_index_pausing_disabled=None,
                          max_parallel_indexers=None,
                          max_parallel_replica_indexers=None,
                          port=None, quota_percent=None, services=None,
                          services_mem_quota_percent=None):
        quota = 0
        init_tasks = list()
        ssh_sessions = dict()

        # Open ssh_connections for command execution
        for server in cluster.servers:
            ssh_sessions[server.ip] = RemoteMachineShellConnection(server)

        for server in cluster.servers:
            # Make sure that data_and index_path are writable by couchbase user
            if self.create_KMIP_secret:
                self.get_KMIP_certificate(server)
            ClusterUtils.flush_network_rules(server)
            if not server.index_path:
                server.index_path = server.data_path
            if not server.cbas_path:
                server.cbas_path = str([server.data_path])
            if not server.eventing_path:
                server.eventing_path = server.data_path
            for path in set([_f for _f in [server.data_path, server.index_path]
                             if _f]):
                for cmd in ("rm -rf {0}/*".format(path),
                            "chown -R couchbase:couchbase {0}".format(path)):
                    ssh_sessions[server.ip].execute_command(cmd)
                rest = RestConnection(server)
                rest.set_data_path(data_path=server.data_path,
                                   index_path=server.index_path,
                                   cbas_path=server.cbas_path)

            if cluster.master != server:
                continue
            init_port = port or server.port or '8091'
            assigned_services = services
            init_tasks.append(
                task.async_init_node(
                    server, disabled_consistent_view,
                    rebalance_index_waiting_disabled,
                    rebalance_index_pausing_disabled,
                    max_parallel_indexers,
                    max_parallel_replica_indexers, init_port,
                    quota_percent, services=assigned_services,
                    gsi_type=self.gsi_type,
                    services_mem_quota_percent=services_mem_quota_percent))
        for _task in init_tasks:
            node_quota = self.task_manager.get_task_result(_task)
            if node_quota < quota or quota == 0:
                quota = node_quota
        if quota < 100 and not len(set([server.ip
                                        for server in self.servers])) == 1:
            self.log.warn("RAM quota was defined less than 100 MB:")
            for server in cluster.servers:
                ram = ssh_sessions[server.ip].extract_remote_info().ram
                self.log.debug("Node: {0}: RAM: {1}".format(server.ip, ram))

        # Close all ssh_connections
        for server in cluster.servers:
            ssh_sessions[server.ip].disconnect()

        if self.jre_path:
            for server in cluster.servers:
                rest = RestConnection(server)
                rest.set_jre_path(self.jre_path)
        return quota

    def bypass_encryption_setting(self):
        self.log.info("Bypassing encryption restrictions")
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.bypass_encryption_restrictions()

    def get_KMIP_certificate(self, node):
        shell = RemoteMachineShellConnection(node)
        try:
            mkdir_command = "mkdir -p " + self.client_certs_path
            shell.execute_command(mkdir_command)
            install_sshpass = "yum install -y sshpass || apt install -y sshpass"
            shell.execute_command(install_sshpass)
            scp_command = (
                "sshpass -p '{pw}' scp -r "
                "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
                "root@{ip}:{path}* "
                "{dest}"
            ).format(
                pw=self.kmip_password,
                ip=self.kmip_ip,
                dest=self.client_certs_path,
                path=self.kmip_cert_path
            )
            output = shell.execute_command(scp_command)
            chown_command = "chown -R couchbase:couchbase " + self.client_certs_path
            shell.execute_command(chown_command)
        finally:
            shell.disconnect()

    def fetch_cb_collect_logs(self):
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        is_single_node_server = len(self.servers) == 1
        for _, cluster in self.cb_clusters.items():
            rest = RestConnection(cluster.master)
            nodes = rest.get_nodes(inactive_added=True,
                                   inactive_failed=True)
            # Creating cluster_util object to handle multi_cluster scenario
            status = self.cluster_util.trigger_cb_collect_on_cluster(
                rest, nodes,
                is_single_node_server)

            if status is True:
                self.cluster_util.wait_for_cb_collect_to_complete(rest)
                self.cluster_util.copy_cb_collect_logs(rest, nodes, cluster,
                                                       log_path)
            else:
                self.log.error("API perform_cb_collect returned False")

    def check_coredump_exist(self, servers, force_collect=False):
        bin_cb = os_constants.Linux.COUCHBASE_BIN_PATH
        lib_cb = os_constants.Linux.COUCHBASE_LIB_PATH
        crash_dir_win = os_constants.Windows.COUCHBASE_CRASH_PATH_RAW
        result = False
        self.data_sets = dict()

        def find_index_of(str_list, sub_string):
            for i in range(len(str_list)):
                if sub_string in str_list[i]:
                    return i
            return -1

        def get_gdb(gdb_shell, dmp_path, dmp_name):
            dmp_file = dmp_path + dmp_name
            core_file = dmp_path + dmp_name.strip(".dmp") + ".core"
            gdb_shell.execute_command("rm -rf " + core_file)
            core_cmd = "/" + bin_cb + "minidump-2-core " + dmp_file + " > " + core_file
            print("running: %s" % core_cmd)
            gdb_shell.execute_command(core_cmd)
            gdb = "gdb --batch {} -c {} -ex \"bt full\" -ex quit"\
                .format(os.path.join(bin_cb, "memcached"), core_file)
            print("running: %s" % gdb)
            result = gdb_shell.execute_command(gdb)[0]
            t_index = find_index_of(result, "Core was generated by")
            result = result[t_index:]
            result = " ".join(result)
            return result

        def get_full_thread_dump(shell):
            cmd = 'gdb -p `(pidof memcached)` -ex "thread apply all bt" -ex detach -ex quit'
            print("running: %s" % cmd)
            thread_dump = shell.execute_command(cmd)[0]
            index = find_index_of(
                thread_dump, "Thread debugging using libthread_db enabled")
            result = " ".join(thread_dump[index:])
            return result

        def run_cbanalyze_core(shell, core_file):
            cbanalyze_core = os.path.join(bin_cb, "tools/cbanalyze-core")
            cmd = '%s %s' % (cbanalyze_core, core_file)
            print("running: %s" % cmd)
            shell.execute_command(cmd)[0]
            cbanalyze_log = core_file + ".log"
            if shell.file_exists(os.path.dirname(cbanalyze_log), os.path.basename(cbanalyze_log)):
                log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
                shell.get_file(os.path.dirname(cbanalyze_log), os.path.basename(cbanalyze_log), log_path)

        def check_logs(grep_output_list):
            """
            Check the grep's last line for the latest timestamp.
            If this timestamp < start_timestamp of the test,
            then return False (as the grep's output is from previous tests)
            Note: This method works only if slave's time(timezone) matches
                  that of VM's. Else it won't be possible to compare timestamps
            """
            last_line = grep_output_list[-1]
            # eg: 2021-07-12T04:03:45
            timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
            match_obj = timestamp_regex.search(last_line)
            if not match_obj:
                # Check if this is a known log entry type that might not have timestamps
                # but shouldn't cause test failures

                # https://jira.issues.couchbase.com/browse/MB-68370
                non_critical_patterns = [
                    "monitorserviceforportchanges"
                ]
                is_non_critical = any(pattern in last_line.lower() for pattern in non_critical_patterns)
                if is_non_critical:
                    self.log.warning("Found log entry without timestamp (treating as non-critical): %s" % last_line)
                    return False  # Don't fail the test for known non-critical log entries
                else:
                    self.log.critical("%s does not match any timestamp" % last_line)
                    return True
            timestamp = match_obj.group()
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
            self.log.info("Comparing timestamps: Log's latest timestamp: %s, "
                          "Test's start timestamp is %s"
                          % (timestamp, self.start_timestamp))
            if timestamp > self.start_timestamp:
                return True
            else:
                return False

        for idx, server in enumerate(servers):
            shell = RemoteMachineShellConnection(server)
            shell.extract_remote_info()
            crash_dir = lib_cb + "crash/"
            if shell.info.type.lower() == "windows":
                crash_dir = crash_dir_win
            if int(server.port) in range(ClusterRun.port,
                                         ClusterRun.port + 10):
                crash_dir = os.path.join(
                    TestInputSingleton.input.servers[0].cli_path,
                    "ns_server", "data",
                    "n_%s" % str(idx), "crash")
            dmp_files = shell.execute_command("ls -lt " + crash_dir)[0]
            dmp_files = [f for f in dmp_files if ".core" not in f]
            dmp_files = [f for f in dmp_files if "total" not in f]
            dmp_files = [f.split()[-1] for f in dmp_files if ".core" not in f]
            dmp_files = [f.strip("\n") for f in dmp_files]
            if dmp_files:
                print("#"*30)
                print("%s: %d core dump seen" % (server.ip, len(dmp_files)))
                print("%s: Stack Trace of first crash - %s\n%s"
                      % (server.ip, dmp_files[-1],
                         get_gdb(shell, crash_dir, dmp_files[-1])))
                print("#"*30)
                result = get_full_thread_dump(shell)
                print(result)
#                 print("#"*30)
#                 run_cbanalyze_core(shell, crash_dir + dmp_files[-1].strip(".dmp") + ".core")
#                 print("#"*30)
                if self.stop_server_on_crash:
                    shell.stop_couchbase()
                result = True
            else:
                self.log.debug(server.ip + ": No crash files found")

            logs_dir = lib_cb + "logs/"
            if int(server.port) in range(ClusterRun.port,
                                         ClusterRun.port + 10):
                logs_dir = os.path.join(
                    TestInputSingleton.input.servers[0].cli_path,
                    "ns_server", "logs", "n_%s" % str(idx))

            # Perform log file searching based on the input yaml config
            yaml = YAML()
            with open("lib/couchbase_helper/error_log_config.yaml", "r") as fp:
                y_data = yaml.load(fp.read())

            for file_data in y_data["file_name_patterns"]:
                log_files = shell.execute_command(
                    "ls " + os.path.join(logs_dir, file_data['file']))[0]

                if len(log_files) == 0:
                    self.log.debug("%s: No '%s' files found"
                                   % (server.ip, file_data['file']))
                    continue

                if 'target_file_index' in file_data:
                    log_files = [
                        log_files[int(file_data['target_file_index'])]
                    ]

                for log_file in log_files:
                    log_file = log_file.strip("\n")
                    pattern_to_grep = []
                    if 'grep_for' in file_data:
                        for grep_pattern_of_log_file in file_data['grep_for']:
                            pattern_to_grep.append(grep_pattern_of_log_file)
                    for common_grep_pattern in y_data["common_patterns"]['grep_for']:
                        pattern_to_grep.append(common_grep_pattern)
                    for grep_pattern in pattern_to_grep:
                        grep_for_str = grep_pattern['string']
                        err_pattern = exclude_pattern = None
                        if 'error_patterns' in grep_pattern:
                            err_pattern = grep_pattern['error_patterns']
                        if 'exclude_patterns' in grep_pattern:
                            exclude_pattern = grep_pattern['exclude_patterns']

                        cmd_to_run = "grep -r '%s' %s" \
                                     % (grep_for_str, log_file)
                        if exclude_pattern is not None:
                            for pattern in exclude_pattern:
                                cmd_to_run += " | grep -v '%s'" % pattern

                        grep_output = shell.execute_command(cmd_to_run)[0]
                        if grep_output and check_logs(grep_output):
                            regex = r"(\bkvstore-\d+)"
                            grep_str = "".join(grep_output)
                            kvstores = list(set(re.findall(regex, grep_str)))
                            self.data_sets[server] = kvstores
                            grep_str = None
                        if err_pattern is not None:
                            for pattern in err_pattern:
                                index = find_index_of(grep_output, pattern)
                                grep_output = grep_output[:index]
                                if grep_output:
                                    self.log.info("unwanted messages in %s" %
                                                  log_file)
                                    if check_logs(grep_output):
                                        self.log.critical(
                                            "%s: Found '%s' logs - %s"
                                            % (server.ip, grep_for_str,
                                               "".join(grep_output)))
                                        result = True
                                        break
                        else:
                            if grep_output \
                                    and check_logs(grep_output):
                                self.log.info("unwanted messages in %s" %
                                              log_file)
                                self.log.critical("%s: Found '%s' logs - %s"
                                                  % (server.ip, grep_for_str,
                                                     grep_output))
                                result = True
                                break
                    if result is True:
                        if self.stop_server_on_crash:
                            shell.stop_couchbase()
                        break

            shell.disconnect()
        if not ClusterRun.is_enabled:
            if result and force_collect and not self.stop_server_on_crash:
                self.fetch_cb_collect_logs()
                self.get_cbcollect_info = False
            if (self.is_test_failed() or result):
                if self.collect_data:
                    self.copy_data_on_slave()
                if self.collect_pcaps:
                    self.log.info("Starting Pcaps collection!!")
                    self.start_fetch_pcaps(is_test_failed=True)

        return result

    def copy_data_on_slave(self, servers=None):
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        if servers is None:
            servers = self.cluster.nodes_in_cluster
            for node in servers:
                if "kv" not in node.services.lower():
                    servers.remove(node)
        if type(servers) is not list:
            servers = [servers]
        remote_path = RestConnection(servers[0]).get_data_path()
        file_path = os.path.join(remote_path, self.cluster.buckets[0].name)
        file_name = self.cluster.buckets[0].name + ".tar.gz"

        def get_tar(remotepath, filepath, filename, servers, todir="."):
            if type(servers) is not list:
                servers = [servers]
            for server in servers:
                shell = RemoteMachineShellConnection(server)
                _ = shell.execute_command("tar -zcvf %s.tar.gz %s" %
                                          (filepath, filepath))
                file_check = shell.file_exists(remotepath, filename)
                if not file_check:
                    self.log.error("Tar File {} doesn't exist".
                                   format(filename))
                tar_file_copied = shell.get_file(remotepath, filename, todir)
                if not tar_file_copied:
                    self.log.error("Failed to copy Tar file")

                _ = shell.execute_command("rm -rf %s.tar.gz" % filepath)

        copy_path_msg_format = "Copying data, Server :: %s, Path :: %s"
        '''
          Temporarily enabling data copy
          of all nodes irrespective of nodes in
          data_sets
        '''
        if False and self.data_sets and self.bucket_storage == "magma":
            self.log.critical("data_sets ==> {}".format(self.data_sets))
            wal_tar = "wal.tar.gz"
            config_json_tar = "config.json.tar.gz"
            for server, kvstores in self.data_sets.items():
                shell = RemoteMachineShellConnection(server)
                if not kvstores:
                    copy_to_path = os.path.join(log_path,
                                                server.ip.replace(".", "_"))
                    if not os.path.isdir(copy_to_path):
                        os.makedirs(copy_to_path, 0o777)
                    self.log.info(copy_path_msg_format % (server.ip,
                                                          copy_to_path))
                    get_tar(remote_path, file_path, file_name,
                            server, todir=copy_to_path)
                else:
                    num_vb = self.vbuckets or CbServer.total_vbuckets
                    for kvstore in kvstores:
                        if int(kvstore.split("-")[1]) >= num_vb:
                            continue
                        kvstore_path = shell.execute_command(
                            "find %s -type d -name '%s'" % (remote_path,
                                                            kvstore))[0][0]
                        magma_dir = kvstore_path.split(kvstore)[0]
                        wal_path = kvstore_path.split(kvstore)[0] + "wal"
                        config_json_path = kvstore_path.split(kvstore)[0]
                        + "config.json"
                        kvstore_path = kvstore_path.split(kvstore)[0] + kvstore
                        kvstore_tar = kvstore + ".tar.gz"
                        copy_to_path = os.path.join(log_path, kvstore)
                        if not os.path.isdir(copy_to_path):
                            os.makedirs(copy_to_path, 0o777)
                        self.log.info(copy_path_msg_format % (server.ip,
                                                              copy_to_path))
                        get_tar(magma_dir, kvstore_path, kvstore_tar,
                                server, todir=copy_to_path)
                        get_tar(magma_dir, wal_path, wal_tar,
                                server, todir=copy_to_path)
                        get_tar(magma_dir, config_json_path, config_json_tar,
                                server, todir=copy_to_path)
        else:
            for server in servers:
                copy_to_path = os.path.join(log_path,
                                            server.ip.replace(".", "_"))
                if not os.path.isdir(copy_to_path):
                    os.makedirs(copy_to_path, 0o777)
                self.log.info(copy_path_msg_format % (server.ip, copy_to_path))
                get_tar(remote_path, file_path, file_name,
                        server, todir=copy_to_path)

    def generate_and_upload_cert(
            self, servers, x509, generate_certs=True, delete_inbox_folder=True,
            upload_root_certs=True, upload_node_certs=True,
            delete_out_of_the_box_CAs=True, upload_client_certs=True):

        if generate_certs:
            x509.generate_multiple_x509_certs(servers=servers)

        if delete_inbox_folder:
            for server in servers:
                x509.delete_inbox_folder_on_server(server=server)

        if upload_root_certs:
            for server in servers:
                _ = x509.upload_root_certs(server)

        if upload_node_certs:
            x509.upload_node_certs(servers=servers)

        if delete_out_of_the_box_CAs:
            for node in servers:
                x509.delete_unused_out_of_the_box_CAs(server=node)

        if upload_client_certs:
            x509.upload_client_cert_settings(server=servers[0])

    def get_executor_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]

    def create_nebula_config(self, nebula_path="",
                             nebula_ip=None, cluster=None):
        import json
        with open(os.path.join(nebula_path, "config.json.example"), "r") as jsonFile:
            data = json.load(jsonFile)
            jsonFile.close()
            data["localHostname"] = nebula_ip
            data["certificates"]["certificate"] = ""
            data["certificates"]["privateKey"] = ""
            data["couchbase"]["certificate"] = ""
            data["couchbase"]["hosts"] = [node.ip for node in cluster.nodes_in_cluster]

        with open(os.path.join(nebula_path, "config.json"), "w") as jsonFile:
            json.dump(data, jsonFile, indent=4)
            jsonFile.close()

        import shutil
        shutil.copy("couchbase_utils/nebula_utils/Dockerfile", nebula_path)

    def handle_setup_exception(self, exception_obj):
        for server in self.input.servers:
            self.set_ports_for_server(server, "non_ssl")
        super(OnPremBaseTest, self).handle_setup_exception(exception_obj)


class ClusterSetup(OnPremBaseTest):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        if self.skip_setup_cleanup:
            self.cluster_util.print_cluster_stats(self.cluster)
            return

        self.log_setup_status("ClusterSetup", "started", "setup")
        self.__initial_rebalance()
        # Add basic RBAC users
        self.bucket_util.add_rbac_user(self.cluster.master)

        # Print cluster stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()

    def __initial_rebalance(self):
        services = None
        if self.services_init:
            services = list()
            for service in self.services_init.split("-"):
                # To handle rebalance-in of serviceless node
                if str(service) == "None":
                    services.append("")
                else:
                    services.append(service.replace(":", ","))
        services = services[1:] \
            if services is not None and len(services) > 1 else None

        # Rebalance-in nodes_init servers
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        if nodes_init:
            result = self.task.rebalance(self.cluster, nodes_init, [],
                                         services=services,
                                         add_nodes_server_groups=None)
            if result is False:
                # Need this block since cb-collect won't be collected
                # in BaseTest if failure happens during setup() stage
                if self.get_cbcollect_info:
                    self.fetch_cb_collect_logs()
                CbServer.use_https = False
                CbServer.n2n_encryption = False
                for server in self.input.servers:
                    self.set_ports_for_server(server, "non_ssl")
                self.fail("Initial rebalance failed")

        if CbServer.cluster_profile == "serverless":
            # Workaround to hitting throttling on serverless config
            RestConnection(self.cluster.master).set_internalSetting("dataThrottleLimit",
                                                                    self.kv_throttling_limit)
            RestConnection(self.cluster.master).set_internalSetting("dataStorageLimit",
                                                                    self.kv_storage_limit)

        # Used to track spare nodes.
        # Test case can use this for further rebalance
        self.spare_nodes = self.servers[self.nodes_init:]

    def create_bucket(self, cluster, bucket_name="default"):
        create_bucket_params = {
            "cluster": cluster,
            "bucket_type": self.bucket_type,
            "ram_quota": self.bucket_size,
            "replica": self.num_replicas,
            "maxTTL": self.bucket_ttl,
            "bucket_rank": self.bucket_rank,
            "compression_mode": self.compression_mode,
            "wait_for_warmup": True,
            "conflict_resolution": Bucket.ConflictResolution.SEQ_NO,
            "replica_index": self.bucket_replica_index,
            "storage": self.bucket_storage,
            "eviction_policy": self.bucket_eviction_policy,
            "flush_enabled": self.flush_enabled,
            "bucket_durability": self.bucket_durability_level,
            "purge_interval": self.bucket_purge_interval,
            "autoCompactionDefined": "false",
            "fragmentation_percentage": 50,
            "bucket_name": bucket_name,
            "width": self.bucket_width,
            "weight": self.bucket_weight,
            "history_retention_collection_default": self.bucket_collection_history_retention_default,
            "history_retention_seconds": self.bucket_dedup_retention_seconds,
            "history_retention_bytes": self.bucket_dedup_retention_bytes,
            "magma_key_tree_data_block_size": self.magma_key_tree_data_block_size,
            "magma_seq_tree_data_block_size": self.magma_seq_tree_data_block_size,
            "enable_encryption_at_rest": self.enable_encryption_at_rest,
            "encryption_at_rest_key_id": self.encryption_at_rest_id,
            "encryption_at_rest_dek_rotation_interval": self.encryptionAtRestDekRotationInterval,
            "encryption_at_rest_dek_lifetime": self.encryption_at_rest_dek_lifetime
        }

        if self.bucket_num_vb is not None:
            create_bucket_params["vbuckets"] = self.bucket_num_vb

        # This is needed because server will throw the error saying,
        # "Support for variable number of vbuckets is not enabled"
        if CbServer.cluster_profile == "serverless":
            create_bucket_params["vbuckets"] = self.vbuckets

        self.bucket_util.create_default_bucket(**create_bucket_params)

        # Add bucket create event in system event log
        if self.system_events.test_start_time is not None:
            bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
            eviction_policy_val = "full_eviction"
            if self.bucket_eviction_policy \
                    == Bucket.EvictionPolicy.VALUE_ONLY:
                eviction_policy_val = "value_only"
            elif self.bucket_eviction_policy \
                    == Bucket.EvictionPolicy.NO_EVICTION:
                eviction_policy_val = "no_eviction"
            bucket_create_event = DataServiceEvents.bucket_create(
                self.cluster.master.ip, self.bucket_type,
                bucket.name, bucket.uuid,
                {'compression_mode': self.compression_mode,
                 'max_ttl': self.bucket_ttl,
                 'storage_mode': self.bucket_storage,
                 'conflict_resolution_type': Bucket.ConflictResolution.SEQ_NO,
                 'eviction_policy': eviction_policy_val,
                 'purge_interval': 'undefined',
                 'durability_min_level': self.bucket_durability_level,
                 'num_replicas': self.num_replicas})
            if self.bucket_type == Bucket.Type.EPHEMERAL:
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props']['storage_mode'] = Bucket.Type.EPHEMERAL
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props'].pop('purge_interval', None)
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props']['eviction_policy'] = "no_eviction"
            self.system_events.add_event(bucket_create_event)
