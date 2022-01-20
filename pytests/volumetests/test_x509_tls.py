import copy
import time

from com.couchbase.client.java import *
from com.couchbase.client.java.json import *
from com.couchbase.client.java.query import *

from Cb_constants import CbServer
from FtsLib.FtsOperations import FtsHelper
from couchbase_utils.cb_tools.cb_cli import CbCli
from couchbase_utils.security_utils.x509main import x509main
from membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
import random
from BucketLib.BucketOperations import BucketHelper
from remote.remote_util import RemoteMachineShellConnection
from error_simulation.cb_error import CouchbaseError
from couchbase_helper.documentgenerator import doc_generator
from security_config import trust_all_certs
from table_view import TableView
from sdk_exceptions import SDKException


class VolumeX509(BaseTestCase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        BaseTestCase.setUp(self)
        self.tasks = []  # To have all tasks running in parallel.
        self._iter_count = 0  # To keep a check of how many items are deleted
        self.available_servers = list()
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.exclude_nodes = [self.cluster.master]
        self.num_buckets = self.input.param("num_buckets", 1)
        self.mutate = 0
        self.doc_ops = self.input.param("doc_ops", None)
        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(';')
        self.iterations = self.input.param("iterations", 2)
        self.vbucket_check = self.input.param("vbucket_check", False)
        self.new_num_writer_threads = self.input.param("new_num_writer_threads", 6)
        self.new_num_reader_threads = self.input.param("new_num_reader_threads", 8)
        self.skip_data_validation = self.input.param("skip_data_validation", True)
        # Services to be added on rebalance-in nodes during the volume test
        self.services_for_rebalance_in = self.input.param("services_for_rebalance_in", None)
        self.log.info("Disabling AF on all nodes before beginning the test")
        for node in self.cluster.servers:
            status = RestConnection(node).update_autofailover_settings(False, 120, False)
            self.assertTrue(status)
        self.x509enable = self.input.param("x509enable", True)
        # Rebalance-in and rebalance-out initially once
        result = self.task.rebalance([self.cluster.master], self.cluster.servers[1:], [])
        if result is False:
            self.fail("Initial rebalance1 failed")
        result = self.task.rebalance(self.cluster.servers, [], self.cluster.servers[1:])
        if result is False:
            self.fail("Initial rebalance2 failed")
        if self.x509enable:
            self.generate_x509_certs()
            self.upload_x509_certs()
        self.log.info("Changing security settings to trust all CAs")
        trust_all_certs()
        self.enable_tls = self.input.param("enable_tls", False)
        if self.enable_tls:
            shell_conn = RemoteMachineShellConnection(self.cluster.master)
            cb_cli = CbCli(shell_conn)
            cb_cli.enable_n2n_encryption()
            RestConnection(self.cluster.master).set_encryption_level(level="strict")
            shell_conn.disconnect()
        self.rest = RestConnection(self.servers[0])
        self.kv_mem_quota = self.input.param("kv_mem_quota", 17000)
        self.index_mem_quota = self.input.param("index_mem_quota", 512)
        self.fts_mem_quota = self.input.param("fts_mem_quota", 512)
        self.set_memory_quota(services=["kv", "index", "fts"])

    def tearDown(self):
        pass

    def index_and_query_setup(self):
        """
        Init index, query objects and create initial indexes
        """
        self.number_of_indexes = self.input.param("number_of_indexes", 0)
        self.n1ql_nodes = None
        if self.number_of_indexes > 0:
            self.n1ql_nodes = self.cluster_util.get_nodes_from_services_map(
                service_type="n1ql",
                get_all_nodes=True,
                servers=self.cluster.servers[:self.nodes_init])
            self.n1ql_rest_connections = list()
            for n1ql_node in self.n1ql_nodes:
                self.n1ql_rest_connections.append(RestConnection(n1ql_node))
            self.n1ql_turn_counter = 0  # To distribute the turn of using n1ql nodes for query. Start with first node

            indexes_to_build = self.create_indexes()
            self.build_deferred_indexes(indexes_to_build)

    def fts_setup(self):
        """
        Create initial fts indexes
        """
        self.fts_indexes_to_create = self.input.param("fts_indexes_to_create", 0)
        self.fts_indexes_to_recreate = self.input.param("fts_indexes_to_recreate", 0)
        if self.fts_indexes_to_create > 0:
            self.create_fts_indexes(self.fts_indexes_to_create)

    def set_memory_quota(self, services=None):
        """
        Set memory quota of services before starting volume steps
        services: list of services for which mem_quota has to be updated
        """
        if services is None:
            return
        if "kv" in services:
            self.rest.set_service_memoryQuota(service='memoryQuota',
                                              memoryQuota=int(self.kv_mem_quota))
        if "index" in services:
            self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                              memoryQuota=int(self.index_mem_quota))
        if "fts" in services:
            self.rest.set_service_memoryQuota(service='ftsMemoryQuota',
                                              memoryQuota=int(self.fts_mem_quota))

    def generate_x509_certs(self):
        """
        Generates x509 root, node, client cert on all servers of the cluster
        """
        # Input parameters for state, path, delimeters and prefixes
        self.client_cert_state = self.input.param("client_cert_state",
                                                  "enable")
        self.paths = self.input.param(
            'paths', "subject.cn:san.dnsname:san.uri").split(":")
        self.prefixs = self.input.param(
            'prefixs', 'www.cb-:us.:www.').split(":")
        self.delimeters = self.input.param('delimeter', '.:.:.').split(":")
        self.setup_once = self.input.param("setup_once", True)
        self.client_ip = self.input.param("client_ip", "172.16.1.174")

        copy_servers = copy.deepcopy(self.cluster.servers)
        x509main(self.cluster.master)._generate_cert(copy_servers, type='openssl',
                                                     encryption='',
                                                     key_length=1024,
                                                     client_ip=self.client_ip)

    def upload_x509_certs(self, servers=None):
        """
        1. Uploads root certs and client-cert settings on servers
        2. Uploads node certs on servers
        """
        if servers is None:
            servers = self.cluster.servers
        self.log.info("Uploading root cert to servers {0}".format(servers))
        for server in servers:
            x509main(server).setup_master(self.client_cert_state, self.paths,
                                          self.prefixs,
                                          self.delimeters)
        self.sleep(5, "Sleeping before uploading node certs to nodes {0}".
                   format(servers))
        x509main().setup_cluster_nodes_ssl(servers, reload_cert=True)

    def x509_reload_after_rebalance_out(self, servers):
        """
        Reloads certs after a node got rebalanced-out
        """
        if not self.x509enable:
            return
        else:
            for server in servers:
                shell_conn = RemoteMachineShellConnection(server)
                cb_cli = CbCli(shell_conn)
                _ = cb_cli.set_n2n_encryption_level(level="control")
                _ = cb_cli.disable_n2n_encryption()
                shell_conn.disconnect()
            self.upload_x509_certs(servers)
            CbServer.use_https = True

    def create_required_buckets(self):
        # Creating buckets for data loading purpose
        self.log.info("Create CB buckets")
        duration = self.input.param("bucket_expiry", 0)
        eviction_policy = self.input.param("eviction_policy", Bucket.EvictionPolicy.VALUE_ONLY)
        self.bucket_type = self.input.param("bucket_type", Bucket.Type.MEMBASE)  # Bucket.bucket_type.EPHEMERAL
        compression_mode = self.input.param("compression_mode",
                                            Bucket.CompressionMode.PASSIVE)  # Bucket.bucket_compression_mode.ACTIVE
        bucket_names = self.input.param("bucket_names", "GleamBookUsers")
        if bucket_names:
            bucket_names = bucket_names.split(';')
        if self.bucket_type:
            self.bucket_type = self.bucket_type.split(';')
        if compression_mode:
            compression_mode = compression_mode.split(';')
        if eviction_policy:
            eviction_policy = eviction_policy.split(';')
        if self.num_buckets == 1:
            bucket = Bucket({"name": "GleamBookUsers", "ramQuotaMB": self.kv_mem_quota, "maxTTL": duration,
                             "replicaNumber": self.num_replicas,
                             "evictionPolicy": eviction_policy[0], "bucketType": self.bucket_type[0],
                             "compressionMode": compression_mode[0]})
            self.bucket_util.create_bucket(bucket)
        elif 1 < self.num_buckets == len(bucket_names):
            for i in range(self.num_buckets):
                bucket = Bucket({"name": bucket_names[i], "ramQuotaMB": self.kv_mem_quota / self.num_buckets, "maxTTL": duration,
                                 "replicaNumber": self.num_replicas,
                                 "evictionPolicy": eviction_policy[i], "bucketType": self.bucket_type[i],
                                 "compressionMode": compression_mode[i]})
                self.bucket_util.create_bucket(bucket)
        else:
            self.fail("Number of bucket/Names not sufficient")

        # rebalance the new buckets across all nodes.
        self.log.info("Rebalance Starts")
        self.nodes = self.rest.node_statuses()
        self.rest.rebalance(otpNodes=[node.id for node in self.nodes],
                            ejectedNodes=[])
        self.rest.monitorRebalance()
        return bucket

    def set_num_writer_and_reader_threads(self, num_writer_threads="default", num_reader_threads="default"):
        for node in self.cluster_util.get_kv_nodes():
            bucket_helper = BucketHelper(node)
            bucket_helper.update_memcached_settings(num_writer_threads=num_writer_threads,
                                                    num_reader_threads=num_reader_threads)

    def volume_doc_generator_users(self, key, start, end):
        template = '{{ "id":"{0}", "alias":"{1}", "name":"{2}", "user_since":"{3}", "employment":{4} }}'
        return GleamBookUsersDocumentGenerator(key, template,
                                               start=start, end=end)

    def volume_doc_generator_messages(self, key, start, end):
        template = '{{ "message_id": "{0}", "author_id": "{1}", "send_time": "{2}" }}'
        return GleamBookMessagesDocumentGenerator(key, template,
                                                  start=start, end=end)

    def initial_data_load(self, initial_load):
        tasks_info = self.bucket_util._async_load_all_buckets(self.cluster, initial_load,
                                                              "create", exp=0,
                                                              persist_to=self.persist_to,
                                                              replicate_to=self.replicate_to,
                                                              batch_size=10,
                                                              pause_secs=5,
                                                              timeout_secs=30,
                                                              durability=self.durability_level,
                                                              process_concurrency=self.process_concurrency,
                                                              retries=self.sdk_retries)

        for task, task_info in tasks_info.items():
            self.task_manager.get_task_result(task)
        self.sleep(10)

    # Loading documents through normal doc loader
    def normal_doc_loader(self):
        tasks_info = dict()
        if "update" in self.doc_ops and self.gen_update_users is not None:
            task_info = self.doc_loader("update", self.gen_update_users)
            tasks_info.update(task_info.items())
        if "create" in self.doc_ops and self.gen_create_users is not None:
            task_info = self.doc_loader("create", self.gen_create_users)
            tasks_info.update(task_info.items())
        if "delete" in self.doc_ops and self.gen_delete_users is not None:
            task_info = self.doc_loader("delete", self.gen_delete_users)
            tasks_info.update(task_info.items())
        return tasks_info

    def doc_loader(self, op_type, kv_gen):
        process_concurrency = self.process_concurrency
        if op_type == "update":
            if "create" not in self.doc_ops:
                self.create_perc = 0
            if "delete" not in self.doc_ops:
                self.delete_perc = 0
            process_concurrency = (self.update_perc * process_concurrency) / (
                    self.create_perc + self.delete_perc + self.update_perc)
        if op_type == "create":
            if "update" not in self.doc_ops:
                self.update_perc = 0
            if "delete" not in self.doc_ops:
                self.delete_perc = 0
            process_concurrency = (self.create_perc * process_concurrency) / (
                    self.create_perc + self.delete_perc + self.update_perc)
        if op_type == "delete":
            if "create" not in self.doc_ops:
                self.create_perc = 0
            if "update" not in self.doc_ops:
                self.update_perc = 0
            process_concurrency = (self.delete_perc * process_concurrency) / (
                    self.create_perc + self.delete_perc + self.update_perc)
        retry_exceptions = [
            SDKException.AmbiguousTimeoutException,
            SDKException.RequestCanceledException,
            SDKException.DurabilityAmbiguousException,
            SDKException.DurabilityImpossibleException,
        ]
        tasks_info = self.bucket_util._async_load_all_buckets(self.cluster, kv_gen,
                                                              op_type, 0, batch_size=20,
                                                              persist_to=self.persist_to,
                                                              replicate_to=self.replicate_to,
                                                              durability=self.durability_level, pause_secs=5,
                                                              timeout_secs=30, process_concurrency=process_concurrency,
                                                              retries=self.sdk_retries,
                                                              retry_exceptions=retry_exceptions)
        return tasks_info

    # Stopping and restarting the memcached process
    def stop_process(self):
        target_node = self.servers[2]
        remote = RemoteMachineShellConnection(target_node)
        error_sim = CouchbaseError(self.log, remote)
        error_to_simulate = "stop_memcached"
        # Induce the error condition
        error_sim.create(error_to_simulate)
        self.sleep(20, "Wait before reverting the error condition")
        # Revert the simulated error condition and close the ssh session
        error_sim.revert(error_to_simulate)
        remote.disconnect()

    def wait_for_failover_or_assert(self, expected_failover_count, timeout=7200):
        # Timeout is kept large for graceful failover
        time_start = time.time()
        time_max_end = time_start + timeout
        actual_failover_count = 0
        while time.time() < time_max_end:
            actual_failover_count = self.get_failover_count()
            if actual_failover_count == expected_failover_count:
                break
            time.sleep(20)
        time_end = time.time()
        if actual_failover_count != expected_failover_count:
            self.log.info(self.rest.print_UI_logs())
        self.assertTrue(actual_failover_count == expected_failover_count,
                        "{0} nodes failed over, expected : {1}"
                        .format(actual_failover_count,
                                expected_failover_count))
        self.log.info("{0} nodes failed over as expected in {1} seconds"
                      .format(actual_failover_count, time_end - time_start))

    def get_failover_count(self):
        rest = RestConnection(self.cluster.master)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

    def rebalance(self, nodes_in=0, nodes_out=0):
        servs_in = random.sample(self.available_servers, nodes_in)

        self.nodes_cluster = self.cluster.nodes_in_cluster[:]
        self.nodes_cluster.remove(self.cluster.master)
        servs_out = random.sample(self.nodes_cluster, nodes_out)

        if nodes_in == nodes_out:
            self.vbucket_check = False

        services = None
        if self.services_for_rebalance_in and nodes_in > 0:
            services = list()
            services.append(self.services_for_rebalance_in.replace(":", ","))
            services = services * nodes_in

        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, servs_out,
            check_vbucket_shuffling=self.vbucket_check,
            services=services)

        self.available_servers = [servs for servs in self.available_servers if servs not in servs_in]
        self.available_servers += servs_out

        self.cluster.nodes_in_cluster.extend(servs_in)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        return rebalance_task, servs_in, servs_out

    def rebalance_validation(self, tasks_info, rebalance_task):
        if not rebalance_task.result:
            for task, _ in tasks_info.items():
                self.task.jython_task_manager.get_task_result(task)
            self.fail("Rebalance Failed")

    def data_validation(self, tasks_info):
        if self.skip_data_validation:
            return
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

            self.sleep(10)

            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        self.log.info("Validating Active/Replica Docs")

        self.check_replica = True

        for bucket in self.bucket_util.buckets:
            tasks = list()
            if self.gen_update_users is not None:
                tasks.append(self.task.async_validate_docs(self.cluster, bucket, self.gen_update_users, "update", 0,
                                                           batch_size=10, check_replica=self.check_replica))
            if self.gen_create_users is not None:
                tasks.append(self.task.async_validate_docs(self.cluster, bucket, self.gen_create_users, "create", 0,
                                                           batch_size=10, check_replica=self.check_replica))
            if self.gen_delete_users is not None:
                tasks.append(self.task.async_validate_docs(self.cluster, bucket, self.gen_delete_users, "delete", 0,
                                                           batch_size=10, check_replica=self.check_replica))
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
            self.sleep(20)

        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(
                self.end - self.initial_load_count * self.delete_perc / 100 * self._iter_count)

    def data_load(self):
        tasks_info = self.normal_doc_loader()
        self.sleep(10)
        return tasks_info

    def generate_docs(self):
        self.create_perc = self.input.param("create_perc", 100)
        self.update_perc = self.input.param("update_perc", 10)
        self.delete_perc = self.input.param("delete_perc", 10)

        self.gen_delete_users = None
        self.gen_create_users = None
        self.gen_update_users = None

        if "update" in self.doc_ops:
            self.mutate += 1
            self.gen_update_users = doc_generator("Users", 0, self.initial_load_count * self.update_perc / 100,
                                                  doc_size=self.doc_size, mutate=self.mutate)
        if "delete" in self.doc_ops:
            self.gen_delete_users = doc_generator("Users", self.start,
                                                  self.start + (self.initial_load_count * self.delete_perc) / 100,
                                                  doc_size=self.doc_size)
            self._iter_count += 1

        if "create" in self.doc_ops:
            self.start = self.end
            self.end += self.initial_load_count * self.create_perc / 100
            self.gen_create_users = doc_generator("Users", self.start, self.end, doc_size=self.doc_size)

    def data_validation_mode(self, tasks_info):
        self.data_validation(tasks_info)

    def get_bucket_dgm(self, bucket):
        self.rest_client = BucketHelper(self.cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket.name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        self.log.info("Active Resident Threshold of {0} is {1}".format(bucket.name, dgm))

    def print_crud_stats(self):
        self.table = TableView(self.log.info)
        self.table.set_headers(["Initial Items", "Current Items", "Items Updated", "Items Created", "Items Deleted"])
        if self._iter_count != 0:
            self.table.add_row(
                [str(self.start - self.initial_load_count * self.delete_perc / 100 * (self._iter_count - 1)),
                 str(self.end - self.initial_load_count * self.delete_perc / 100 * self._iter_count),
                 str(self.update_perc - self.update_perc) + "---" +
                 str(self.initial_load_count * self.update_perc / 100),
                 str(self.start) + "---" + str(self.end),
                 str(self.start - self.initial_load_count * self.create_perc / 100) + "---" +
                 str(self.start + (
                         self.initial_load_count * self.delete_perc / 100) - self.initial_load_count * self.create_perc / 100)])
        self.table.display("Docs statistics")

    def run_cbq_query(self, query):
        """
        To run cbq queries
        Note: Do not run this in parallel
        """
        result = self.n1ql_rest_connections[self.n1ql_turn_counter].query_tool(query, timeout=1300)
        self.n1ql_turn_counter = (self.n1ql_turn_counter + 1) % len(self.n1ql_nodes)
        return result

    def wait_for_indexes_to_go_online(self, gsi_index_names, timeout=300):
        """
        Wait for indexes to go online after building the deferred indexes
        """
        self.log.info("Waiting for indexes to go online")
        start_time = time.time()
        stop_time = start_time + timeout
        for gsi_index_name in gsi_index_names:
            while True:
                check_state_query = "SELECT state FROM system:indexes WHERE name='%s'" % gsi_index_name
                result = self.run_cbq_query(check_state_query)
                if result['results'][0]['state'] == "online":
                    break
                if time.time() > stop_time:
                    self.fail("Index availability timeout of index: {0}".format(gsi_index_name))

    def build_deferred_indexes(self, indexes_to_build):
        """
        Build secondary indexes that were deferred
        """
        self.log.info("Building indexes")
        for bucket, gsi_index_names in indexes_to_build.items():
            for iterator in range(0, len(gsi_index_names), 10):
                if iterator+10 <= len(gsi_index_names):
                    indexes = gsi_index_names[iterator:iterator+10]
                else:
                    indexes = gsi_index_names[iterator:]
                build_query = "BUILD INDEX on `%s`(%s) " \
                              "USING GSI" \
                              % (bucket, indexes)
                result = self.run_cbq_query(build_query)
                self.assertTrue(result['status'] == "success", "Build query %s failed." % build_query)
                self.wait_for_indexes_to_go_online(indexes)

        query = "select state from system:indexes where state='deferred'"
        result = self.run_cbq_query(query)
        self.log.info("deferred indexes remaining: {0}".format(len(result['results'])))
        query = "select state from system:indexes where state='online'"
        result = self.run_cbq_query(query)
        self.log.info("online indexes count: {0}".format(len(result['results'])))
        self.sleep(60, "Wait after building indexes")

    def create_indexes(self):
        """
        Create gsi indexes on collections - according to number_of_indexes
        """
        self.log.info("Creating indexes with defer build")
        indexes_to_build = dict()
        count = 0
        couchbase_buckets = [bucket for bucket in self.bucket_util.buckets if bucket.bucketType == "couchbase"]
        for bucket in couchbase_buckets:
            indexes_to_build[bucket.name] = list()
            while count < self.number_of_indexes:
                gsi_index_name = "gsi-" + str(count)
                create_index_query = "CREATE INDEX `%s` " \
                                     "ON `%s`(`age`)" \
                                     "WITH { 'defer_build': true, 'num_replica': 0 }" \
                                     % (gsi_index_name, bucket.name)
                result = self.run_cbq_query(create_index_query)
                # self.assertTrue(result['status'] == "success", "Defer build Query %s failed." % create_index_query)
                indexes_to_build[bucket.name].append(gsi_index_name)
                count = count + 1
        return indexes_to_build

    @staticmethod
    def get_fts_param_template():
        fts_param_template = '{' \
                             '"name": "%s",' \
                             '"type": "fulltext-index",' \
                             '"params": {' \
                             '"mapping": {' \
                             '"default_mapping": {' \
                             '"enabled": true,' \
                             '"dynamic": true},' \
                             '"default_type": "_default",' \
                             '"default_analyzer": "standard",' \
                             '"default_datetime_parser": "dateTimeOptional",' \
                             '"default_field": "_all",' \
                             '"store_dynamic": false,' \
                             '"index_dynamic": true},' \
                             '"store": {' \
                             '"indexType": "scorch",' \
                             '"kvStoreName": ""' \
                             '},' \
                             '"doc_config": {' \
                             '"mode": "type_field",' \
                             '"type_field": "type",' \
                             '"docid_prefix_delim": "",' \
                             '"docid_regexp": ""}},' \
                             '"sourceType": "couchbase",' \
                             '"sourceName": "%s",' \
                             '"sourceParams": {},' \
                             '"planParams": {' \
                             '"maxPartitionsPerPIndex": 171,' \
                             '"numReplicas": 0,' \
                             '"indexPartitions": 6},"uuid": ""}'
        return fts_param_template

    def create_fts_indexes(self, count=100, base_name="fts"):
        """
        Creates count number of fts indexes on collections
        count should be less than number of collections
        """
        self.log.info("Creating {} fts indexes ".format(count))
        fts_helper = FtsHelper(self.cluster_util.get_nodes_from_services_map(
            service_type="fts",
            get_all_nodes=False))
        couchbase_buckets = [bucket for bucket in self.bucket_util.buckets if bucket.bucketType == "couchbase"]
        created_count = 0
        fts_indexes = dict()
        for bucket in couchbase_buckets:
            fts_indexes[bucket.name] = list()
            while created_count < count:
                fts_index_name = base_name + str(created_count)
                fts_param_template = self.get_fts_param_template()
                status, content = fts_helper.create_fts_index_from_json(
                    fts_index_name,
                    fts_param_template % (fts_index_name,
                                          bucket.name))

                if status is False:
                    self.fail("Failed to create fts index %s: %s"
                              % (fts_index_name, content))
                fts_indexes[bucket.name].append(fts_index_name)
                created_count = created_count + 1

    def test_volume_taf(self):
        ########################################################################################################################
        self.log.info("Step1: Create a n node cluster")
        services = None
        if self.services_init:
            services = list()
            for service in self.services_init.split("-"):
                services.append(service.replace(":", ","))
        services = services[1:] \
            if services is not None and len(services) > 1 else None
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        if nodes_init:
            result = self.task.rebalance([self.cluster.master], nodes_init, [],
                                         services=services)
            if result is False:
                self.fail("Initial rebalance3 failed")
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.query_node = self.cluster.master
        ########################################################################################################################
        self.log.info("Step 2 & 3: Create required buckets.")
        bucket = self.create_required_buckets()
        self.loop = 0
        ########################################################################################################################
        self.log.info("Step 4a: Creating fts/gsi indexes")
        self.index_and_query_setup()
        self.fts_setup()
        #######################################################################################################################
        while self.loop < self.iterations:
            self.log.info("Step 4b: Pre-Requisites for Loading of docs")
            self.start = 0
            self.bucket_util.add_rbac_user()
            self.end = self.initial_load_count = self.input.param("initial_load", 1000)
            initial_load = doc_generator("Users", self.start, self.start + self.initial_load_count,
                                         doc_size=self.doc_size)
            self.initial_data_load(initial_load)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 5: Rebalance in with Loading of docs")
            self.generate_docs()
            self.gen_delete_users = None
            self._iter_count = 0
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task, _, _ = self.rebalance(nodes_in=1, nodes_out=0)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            #########################################################################################################################
            self.log.info("Step 6: Rebalance Out with Loading of docs")
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task, _, servs_out = self.rebalance(nodes_in=0, nodes_out=1)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            self.x509_reload_after_rebalance_out(servs_out)
            ########################################################################################################################
            self.log.info("Step 8: Swap with Loading of docs")
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task, servs_in, servs_out = self.rebalance(nodes_in=1, nodes_out=1)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            self.x509_reload_after_rebalance_out(servs_out)
            ########################################################################################################################
            self.log.info("Step 9: Updating the bucket replica to 2 and rebalance in")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=2)
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            rebalance_task, _, _ = self.rebalance(nodes_in=1, nodes_out=0)
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.sleep(600, "Wait for Rebalance to start")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            if "ephemeral" in self.bucket_type:
                self.log.info("No Memcached kill for epehemral bucket")
            else:
                self.log.info("Step 10: Stopping and restarting memcached process")
                self.generate_docs()
                if not self.atomicity:
                    self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                           num_reader_threads=self.new_num_reader_threads)
                rebalance_task = self.task.async_rebalance(self.cluster.servers, [], [])
                tasks_info = self.data_load()
                if not self.atomicity:
                    self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                           num_reader_threads="disk_io_optimized")
                # self.sleep(600, "Wait for Rebalance to start")
                self.task.jython_task_manager.get_task_result(rebalance_task)
                reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
                self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                self.stop_process()
                self.data_validation_mode(tasks_info)
                self.tasks = []
                self.bucket_util.print_bucket_stats()
                self.print_crud_stats()
                self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 11: Failover a node and RebalanceOut that node with loading in parallel")

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)

            # Mark Node for failover
            self.generate_docs()
            tasks_info = self.data_load()
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id, graceful=False)
            self.wait_for_failover_or_assert(expected_failover_count=1)
            self.sleep(60, "wait for failover")
            self.nodes = self.rest.node_statuses()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            # self.rest.rebalance(otpNodes=[node.id for node in self.nodes], ejectedNodes=[self.chosen[0].id])
            # self.assertTrue(self.rest.monitorRebalance(stop_if_loop=True), msg="Rebalance failed")
            result = self.task.rebalance(self.cluster.nodes_in_cluster, [], [])
            if result is False:
                self.fail("rebalance failed")

            servs_out = [node for node in self.cluster.servers if node.ip == self.chosen[0].ip]
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
            self.available_servers += servs_out
            self.sleep(10)

            self.data_validation_mode(tasks_info)

            self.sleep(10)
            self.tasks = []
            self.x509_reload_after_rebalance_out(servs_out)
            rebalance_task, _, _ = self.rebalance(nodes_in=1, nodes_out=0)
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 12: Failover a node and FullRecovery that node")

            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)

            self.generate_docs()
            tasks_info = self.data_load()
            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id, graceful=False)
            self.wait_for_failover_or_assert(expected_failover_count=1)
            self.sleep(60, "wait after failover")

            # Mark Node for full recovery
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id, recoveryType="full")

            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.sleep(10)

            self.data_validation_mode(tasks_info)

            self.sleep(10)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 13: Failover a node and DeltaRecovery that node with loading in parallel")

            self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)

            self.rest = RestConnection(self.cluster.master)
            self.nodes = self.cluster_util.get_nodes(self.cluster.master)
            self.chosen = self.cluster_util.pick_nodes(self.cluster.master, howmany=1)

            self.generate_docs()
            tasks_info = self.data_load()
            # Mark Node for failover
            self.success_failed_over = self.rest.fail_over(self.chosen[0].id, graceful=False)

            self.wait_for_failover_or_assert(expected_failover_count=1)
            self.sleep(60, "wait after failover")
            if self.success_failed_over:
                self.rest.set_recovery_type(otpNode=self.chosen[0].id, recoveryType="delta")
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)

            rebalance_task = self.task.async_rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.sleep(10)

            self.data_validation_mode(tasks_info)

            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 14: Updating the bucket replica to 1")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=1)
            self.generate_docs()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads=self.new_num_writer_threads,
                                                       num_reader_threads=self.new_num_reader_threads)
            rebalance_task = self.task.async_rebalance(self.cluster.servers, [], [])
            tasks_info = self.data_load()
            if not self.atomicity:
                self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                       num_reader_threads="disk_io_optimized")
            self.task.jython_task_manager.get_task_result(rebalance_task)
            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.assertTrue(reached, "rebalance failed, stuck or did not complete")
            self.data_validation_mode(tasks_info)
            self.tasks = []
            self.bucket_util.print_bucket_stats()
            self.print_crud_stats()
            self.get_bucket_dgm(bucket)
            ########################################################################################################################
            self.log.info("Step 15: Flush the bucket and start the entire process again")
            self.loop += 1
            if self.loop < self.iterations:
                # Flush the bucket
                self.bucket_util.flush_all_buckets(self.cluster.master)
                self.sleep(10)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    self.nodes_cluster = self.cluster.nodes_in_cluster[:]
                    self.nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(self.nodes_cluster,
                                              int(len(self.cluster.nodes_in_cluster) - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out)
                    self.task.jython_task_manager.get_task_result(rebalance_task)
                    self.available_servers += servs_out
                    self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
                    reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
                    self.assertTrue(reached, "rebalance failed, stuck or did not complete")
                    self.get_bucket_dgm(bucket)
                self._iter_count = 0
            else:
                self.log.info("Volume Test Run Complete")
                self.get_bucket_dgm(bucket)
        ############################################################################################################################
