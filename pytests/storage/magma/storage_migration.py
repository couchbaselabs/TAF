from BucketLib.bucket import Bucket
from Jython_tasks.task_manager import TaskManager
from basetestcase import BaseTestCase
from common_lib import sleep
from couchbase_helper.cluster import ServerTasks
from membase.api.rest_client import RestConnection
from couchbase_helper.documentgenerator import doc_generator
from xdcr_utils.xdcr_ready_functions import XDCRUtils
from cb_constants.CBServer import CbServer


class StorageMigration(BaseTestCase):

    def setUp(self):
        super(StorageMigration, self).setUp()
        self.migration_with_xdcr = self.input.param("migration_with_xdcr", True)
        self.source_bucket_name = self.input.param("source_bucket_name", "source_bucket")
        self.target_bucket_name = self.input.param("target_bucket_name", "target_bucket")
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.item_count = self.input.param("item_count", 15000000)

        self.current_storage = self.input.param("current_storage", Bucket.StorageBackend.couchstore)
        self.preferred_storage = self.input.param("preferred_storage", Bucket.StorageBackend.magma)

        self.retry_get_process_num = self.input.param("retry_get_process_num", 100)

        self.num_nodes_in_cluster = self.input.param("num_nodes_in_cluster", 2)

        self.remotePath = self.input.param("remotePath",
                                           "/opt/couchbase/var/lib/couchbase/config/certs/")
        self.localPath = self.input.param("localPath", "/couchbase_certs/")
        self.cert_name = self.input.param("cert_name", "ca.pem")

        self.clusters = self.get_clusters()

        for cluster in self.clusters:
            nodes_in = cluster.servers[1:self.nodes_init]
            if self.services_init is None:
                self.services = ["kv"] * self.nodes_init
            else:
                self.services = self.services_init.replace(":", ",").split("-")

            result = self.task.rebalance(cluster, nodes_in, [],
                                        services=self.services[1:])
            self.assertTrue(result, "Initial rebalance failed")
            for idx, node in enumerate(cluster.nodes_in_cluster):
                node.services = self.services[idx]

        if self.migration_with_xdcr:
            xdcr_task_manager = TaskManager(10)
            xdcr_task = ServerTasks(xdcr_task_manager)
            self.xdcr_util = XDCRUtils(self.clusters, xdcr_task, xdcr_task_manager, False)
            self.source_cluster = self.xdcr_util.get_cb_cluster_by_name("C1")
            self.destination_cluster = self.xdcr_util.get_cb_cluster_by_name("C2")

            self.source_cluster.nodes_in_cluster = \
                self.cluster_util.get_nodes_in_cluster(self.source_cluster)
            self.destination_cluster.nodes_in_cluster = \
                self.cluster_util.get_nodes_in_cluster(self.destination_cluster)
            self.source_cluster.kv_nodes = self.cluster_util.get_kv_nodes(self.source_cluster)
            self.destination_cluster.kv_nodes = \
                self.cluster_util.get_kv_nodes(self.destination_cluster)
            self.spare_node = self.source_cluster.servers[self.nodes_init]

        if self.enforce_tls:
            self.log.info("Enabling TLS on both clusters")
            self.enable_tls_on_nodes()
            for cluster in self.clusters:
                for server in cluster.servers:
                    self.set_ports_for_server(server, "ssl")

        for cluster in self.clusters:
            self.cluster_util.print_cluster_stats(cluster)

    def tearDown(self):
        super(StorageMigration, self).tearDown()


    def test_storage_migration_with_xdcr(self):

        self.log.info("Creating a bucket: {} in the source cluster".format(
                        self.source_bucket_name))
        self.bucket_util.create_default_bucket(
                    self.source_cluster,
                    ram_quota=self.bucket_ram_quota,
                    replica=self.num_replicas,
                    storage=self.bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    bucket_name=self.source_bucket_name)

        self.log.info("Creating a bucket: {} in the destination cluster".format(
                        self.target_bucket_name))
        self.bucket_util.create_default_bucket(
                    self.destination_cluster,
                    ram_quota=self.bucket_ram_quota,
                    replica=self.num_replicas,
                    storage=self.bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    bucket_name=self.target_bucket_name)

        self.bucket_util.print_bucket_stats(self.source_cluster)
        self.bucket_util.print_bucket_stats(self.destination_cluster)

        rest = RestConnection(self.source_cluster.master)

        # Removing all the previous replications before starting a new replication
        rest.remove_all_replications()

        if self.enforce_tls and self.encryption_level == "strict":
            self.xdcr_util.copy_cert_to_slave(self.destination_cluster.master,
                                              self.remotePath, self.localPath)
            cert_val = self.xdcr_util.get_cert_value(self.localPath, self.cert_name)
            self.log.info("Certificate val = {}".format(cert_val))

            rest.add_remote_cluster(
                self.destination_cluster.master.ip,
                self.destination_cluster.master.port,
                self.destination_cluster.master.rest_username,
                self.destination_cluster.master.rest_password,
                self.destination_cluster.master.ip,
                demandEncryption=1, certificate=cert_val, secureType='full')

        else:
            rest.add_remote_cluster(
                self.destination_cluster.master.ip,
                self.destination_cluster.master.port,
                self.destination_cluster.master.rest_username,
                self.destination_cluster.master.rest_password,
                self.destination_cluster.master.ip)

        self.log.info("Starting XDCR from bucket: {0} to bucket: {1}".format(
                        self.source_bucket_name, self.target_bucket_name))
        rest.start_replication("continuous", self.source_bucket_name,
                               self.destination_cluster.master.ip,
                               toBucket=self.target_bucket_name)

        self.log.info("Starting a data load on the source bucket")
        doc_gen = doc_generator(key="temp_docs", start=0,
                                end=self.item_count, doc_size=1024)
        load_task = self.task.async_load_gen_docs(
            self.source_cluster, self.source_cluster.buckets[0], doc_gen,
            "create", load_using=self.load_docs_using)

        self.log.info("Starting migration of storageBackend for bucket: {}".format(
                            self.source_bucket_name))
        self.bucket_util.update_bucket_property(self.source_cluster.master,
                                        self.source_cluster.buckets[0],
                                        storageBackend=Bucket.StorageBackend.magma)

        nodes_to_migrate = self.source_cluster.nodes_in_cluster
        for node in nodes_to_migrate:
            self.log.info("Node to migrate = {}".format(node.ip))
            self.log.info("Swap Rebalance starting...")
            rebalance_res = self.task.rebalance(self.source_cluster,
                                        to_add=[self.spare_node],
                                        to_remove=[node],
                                        check_vbucket_shuffling=False,
                                        services=["kv"],
                                        retry_get_process_num=self.retry_get_process_num)
            self.assertTrue(rebalance_res, "Swap rebalance failed")
            self.spare_node = node

            self.cluster_util.print_cluster_stats(self.source_cluster)

        self.log.info("Stopping doc loading after the completion of migration")
        self.task_manager.abort_all_tasks()

        self.log.info("Enabling CDC after storage migration")
        res = self.bucket_util.update_bucket_property(self.source_cluster.master,
                                                    self.source_cluster.buckets[0],
                                                    history_retention_seconds=86400,
                                                    history_retention_bytes=96000000000)
        self.assertTrue(res, "Enabling CDC failed")

        sleep(300, "Wait for replication to catch up")
        source_bucket_item_count = self.bucket_util.get_buckets_item_count(
            self.source_cluster, self.source_cluster.buckets[0].name)
        target_bucket_item_count = self.bucket_util.get_buckets_item_count(
            self.destination_cluster, self.destination_cluster.buckets[0].name)
        err_msg = "Item count mismatch. Source bucket item count: {0}, " \
                    "Target bucket item count: {1}".format(
                    source_bucket_item_count, target_bucket_item_count)
        self.assertEqual(source_bucket_item_count, target_bucket_item_count, err_msg)
        self.log.info("Validated bucket item count on the source and remote cluster")


    def test_migration_scenarios(self):

        self.cluster = self.clusters[0]

        # Create a bucket
        bucket_name = self.current_storage + "_bucket"
        self.bucket_util.create_default_bucket(
                    self.cluster,
                    ram_quota=self.bucket_ram_quota,
                    storage=self.current_storage,
                    bucket_name=bucket_name)

        bucket_to_migrate = [bucket for bucket in self.cluster.buckets
                            if bucket.name == bucket_name][0]

        if self.preferred_storage == Bucket.StorageBackend.couchstore:
            self.log.info("Disabling history before migrating to CouchStore")
            self.bucket_util.update_bucket_property(
                                self.cluster.master,
                                bucket_to_migrate,
                                history_retention_collection_default="false")
            self.bucket_util.set_history_retention_for_collection(self.cluster.master,
                                    bucket_to_migrate, "_default", "_default", "false")

        # Attempt to migrate the bucket
        msg = "Migration from {0} {1}MB {2}vB to {3} {1}MB {2}vB".format(self.current_storage,
                                self.bucket_ram_quota, self.vbuckets, self.preferred_storage)
        try:
            self.bucket_util.update_bucket_property(
                                self.cluster.master,
                                bucket_to_migrate,
                                storageBackend=self.preferred_storage)
        except Exception as e:
            self.log.info(e)
            self.log.info(msg + "failed as expected")
        else:
            self.fail(msg + "succeeded")