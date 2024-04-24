from basetestcase import BaseTestCase
from cb_constants import CbServer
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class MagmaRecovery(BaseTestCase):
    def setUp(self):
        super(MagmaRecovery, self).setUp()

        self.bucket_name = self.input.param("bucket_name", "default")
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.item_count = self.input.param("item_count", 1000000)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.num_nodes_in_cluster = self.input.param("num_nodes_in_cluster", 2)
        self.scope_to_load = self.input.param("scope_to_load", "_default")
        self.collection_to_load = self.input.param("collection_to_load", "_default")
        self.include_single_bucket = self.input.param("include_single_bucket", False)
        self.bucket_to_include = self.input.param("bucket_to_include", "default1")
        self.include_vbucket_filter = self.input.param("include_vbucket_filter", False)
        self.test_auto_create_collections = self.input.param("test_auto_create_collections", False)
        self.make_node_offline = self.input.param("make_node_offline", False)
        self.transfer_replica_vbuckets = self.input.param("transfer_replica_vbuckets", False)
        self.transfer_dead_vbuckets = self.input.param("transfer_dead_vbuckets", False)
        self.ssl_no_verify = self.input.param("ssl_no_verify", False)
        self.username = self.input.param("username", "Administrator")
        self.password = self.input.param("password", "password")

        self.clusters = self.get_clusters()
        if self.num_nodes_in_cluster > 1:
            for cluster in self.clusters:
                self.cluster_util.add_all_nodes_then_rebalance(
                    cluster, cluster.servers[1:])
        self.first_cluster = self.get_cluster_by_name('C1')
        self.second_cluster = self.get_cluster_by_name('C2')
        self.first_cluster_master = self.first_cluster.master
        self.second_cluster_master = self.second_cluster.master
        self.first_cluster.nodes_in_cluster = self.cluster_util.get_nodes_in_cluster(self.first_cluster)
        self.second_cluster.nodes_in_cluster = self.cluster_util.get_nodes_in_cluster(self.second_cluster)

        for cluster in self.clusters:
            self.cluster_util.print_cluster_stats(cluster)

    def tearDown(self):
        super(MagmaRecovery, self).tearDown()

    def get_cluster_by_name(self, cluster_name):
        for cb_cluster in self.clusters:
            if cb_cluster.name == cluster_name:
                return cb_cluster


    def test_cbdatarecovery_with_magma(self):

        for i in range(1,self.num_buckets+1):
            bucket_name = self.bucket_name + str(i)
            self.log.info("Creating a bucket:{0} in cluster {1}".format(bucket_name,
                                                            self.first_cluster.name))
            self.bucket_util.create_default_bucket(
                    self.first_cluster,
                    ram_quota=self.bucket_ram_quota,
                    replica=self.num_replicas,
                    storage=self.bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    bucket_name=bucket_name)
        self.bucket_util.print_bucket_stats(self.first_cluster)

        if self.test_auto_create_collections:
            for bucket in self.first_cluster.buckets:
                self.log.info("Creating scope: {0} in bucket:{1}".format(self.scope_to_load,
                                                                         bucket.name))
                self.bucket_util.create_scope(self.first_cluster_master,
                                              bucket,
                                              {"name": self.scope_to_load})
                self.log.info("Creating collection:{0} in scope:{1}".format(self.collection_to_load,
                                                                            self.scope_to_load))
                self.bucket_util.create_collection(
                        self.first_cluster_master, bucket,
                        self.scope_to_load, {"name": self.collection_to_load})

        for bucket in self.first_cluster.buckets:
            self.log.info("Loading data into bucket: {}".format(bucket.name))
            doc_gen = doc_generator(key="temp_docs", start=0, end=self.item_count, doc_size=1024)
            doc_loading_task = self.task.async_load_gen_docs(
                self.first_cluster, bucket, doc_gen, "create",
                scope=self.scope_to_load, collection=self.collection_to_load,
                load_using=self.load_docs_using)
            self.task_manager.get_task_result(doc_loading_task)

        self.sleep(30, "Wait for bucket item count to get reflected")
        self.bucket_util.print_bucket_stats(self.first_cluster)
        initial_bucket_count = {}
        for bucket in self.first_cluster.buckets:
            item_count = self.bucket_util.get_bucket_current_item_count(self.first_cluster, bucket)
            initial_bucket_count[bucket.name] = item_count
        self.log.info("Cluster {0} bucket item count {1}".format(self.first_cluster.name,
                                                                 initial_bucket_count))

        for i in range(1,self.num_buckets+1):
            bucket_name = self.bucket_name + str(i)
            self.log.info("Creating a bucket:{0} in cluster {1}".format(bucket_name,
                                                                self.second_cluster.name))
            self.bucket_util.create_default_bucket(
                    self.second_cluster,
                    ram_quota=self.bucket_ram_quota,
                    replica=self.num_replicas,
                    storage=self.bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    bucket_name=bucket_name)
        self.bucket_util.print_bucket_stats(self.second_cluster)

        data_path = RestConnection(self.first_cluster_master).get_data_path()

        if not self.include_single_bucket:
            buckets_to_recover = self.second_cluster.buckets
        else:
            buckets_to_recover = [bucket for bucket in self.second_cluster.buckets
                                  if bucket.name == self.bucket_to_include]

        if self.make_node_offline:
            self.log.info("Stopping couchbase server to make nodes offline")
            for node in self.first_cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.stop_couchbase()
            self.sleep(20, "Wait for a few second after stopping Couchbase on nodes")

        if self.transfer_dead_vbuckets:
            shell = RemoteMachineShellConnection(self.first_cluster_master)
            time_interval = 600000
            self.log.info("Changing Janitor time interval to {}".format(time_interval))
            if CbServer.use_https:
                prefix = "-k https"
                port_to_use = 18091
            else:
                prefix = "http"
                port_to_use = 8091
            janitor_command = "curl {0}://{1}:{2}@localhost:{3}/diag/eval -X POST -d " \
                    .format(prefix, self.username, self.password, port_to_use)
            janitor_command += "'ns_config:set({ns_orchestrator, janitor_interval}, 600000)'"
            self.log.info("Running command = {}".format(janitor_command))
            output, error = shell.execute_command(janitor_command)
            self.sleep(2)
            number_of_vbuckets = self.input.param("number_of_vbuckets", 100)
            self.log.info("Setting state=dead for {} vbuckets through diag/eval".format(number_of_vbuckets))
            for vbucket_no in range(number_of_vbuckets):
                command = "curl {0}://{1}:{2}@localhost:{3}/diag/eval -X POST -d ".format(
                    prefix, self.username, self.password, port_to_use)
                command += "'ns_memcached:set_vbucket(\"{0}\", {1}, dead)'" \
                        .format(self.first_cluster.buckets[0].name, vbucket_no)
                output, error = shell.execute_command(command)
            shell.disconnect()
            self.sleep(30, "Wait for a few seconds after changing the state of vbuckets")
            bucket_item_count = self.bucket_util.get_bucket_current_item_count(self.first_cluster,
                                                                    self.first_cluster.buckets[0])
            self.log.info("Bucket item count after changing" \
                          "a few vbuckets state to dead = {}".format(bucket_item_count))
            items_to_transfer = self.item_count - bucket_item_count

        for server in self.first_cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            if self.encryption_level == "strict":
                recovery_cmd = '{0}cbdatarecovery -c https://{1}:18091 -u {2} -p {3} -d {4}'.format(
                    shell.return_bin_path_based_on_os(shell.return_os_type()),
                    self.second_cluster_master.ip, self.username, self.password, data_path)
            else:
                recovery_cmd = '{0}cbdatarecovery -c {1} -u {2} -p {3} -d {4}'.format(
                    shell.return_bin_path_based_on_os(shell.return_os_type()),
                    self.second_cluster_master.ip, self.username, self.password, data_path)
            if self.include_single_bucket:
                recovery_cmd += ' --include-data {}'.format(self.bucket_to_include)
            if self.include_vbucket_filter:
                recovery_cmd += ' --vbucket-filter 0-511'
            if self.test_auto_create_collections:
                recovery_cmd += ' --auto-create-collections'
            if self.transfer_replica_vbuckets:
                recovery_cmd += ' --vbucket-state replica'
            if self.transfer_dead_vbuckets:
                recovery_cmd += ' --vbucket-state dead'
            if self.ssl_no_verify:
                recovery_cmd += ' --no-ssl-verify'

            self.log.info("Running command = {}".format(recovery_cmd))
            o, r = shell.execute_command(recovery_cmd)
            shell.log_command_output(o, r)
            self.assertTrue('Recovery completed successfully' in ''.join(o),
                                msg='Recovery was unsuccessful')
            self.log.info("Recovery was successful on node {}".format(server.ip))
            shell.disconnect()

        self.sleep(30, "Wait for bucket item count to get reflected")
        self.bucket_util.print_bucket_stats(self.second_cluster)
        self.log.info("Verifying bucket item count after recovery")
        for bucket in buckets_to_recover:
            actual_count = self.bucket_util.get_bucket_current_item_count(self.second_cluster,
                                                                          bucket)
            expected_count = initial_bucket_count[bucket.name]
            if self.include_vbucket_filter:
                expected_count = initial_bucket_count[bucket.name] // 2
            if self.transfer_dead_vbuckets:
                expected_count = items_to_transfer
            err_msg = "Bucket item count does not match for bucket:{}".format(bucket.name)
            err_msg += " Expected: {0}, Actual: {1}".format(expected_count, actual_count)
            self.assertTrue(actual_count == expected_count, err_msg)
            self.log.info("Bucket item count verified for bucket:{}".format(bucket.name))

        if self.make_node_offline:
            self.log.info("Re-starting couchbase server to bring nodes back up")
            for node in self.first_cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.start_couchbase()
