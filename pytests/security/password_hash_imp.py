import json

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.security.security_api import SecurityRestAPI
from pytests.basetestcase import ClusterSetup
from pytests.bucket_collections.collections_base import CollectionBase
from rebalance_utils.rebalance_util import RebalanceUtil
from security_utils.security_utils import SecurityUtils
from membase.api.rest_client import RestConnection
from cb_constants import CbServer
from couchbase_utils.security_utils.x509_multiple_CA_util import x509main


class PasswordHashImp(ClusterSetup):
    def setUp(self):
        super(PasswordHashImp, self).setUp()
        self.security_util = SecurityUtils(self.log)
        self.spec_name = self.input.param("bucket_spec",
                                          "single_bucket.bucket_for_magma_collections")
        self.initial_data_spec = self.input.param("initial_data_spec", "initial_load")
        self.rebalance_type = self.input.param("rebalance_type", "rebalance_in")
        self.force_rotate = self.input.param("force_rotate", True)
        self.num_node_failures = self.input.param("num_node_failures", 1)
        self.servers_to_fail = self.cluster.servers[1:self.num_node_failures + 1]
        self.current_fo_strategy = self.input.param("current_fo_strategy", "auto")
        self.standard = self.input.param("standard", "pkcs8")
        self.passphrase_type = self.input.param("passphrase_type", "script")
        self.encryption_type = self.input.param("encryption_type", "aes256")
        self.x509 = x509main(host=self.cluster.master, standard=self.standard,
                             encryption_type=self.encryption_type,
                             passphrase_type=self.passphrase_type)
        self.rest = ClusterRestAPI(self.cluster.master)
        self.rebalance_util = RebalanceUtil(self.cluster)

        # Creating buckets from spec file
        CollectionBase.deploy_buckets_from_spec_file(self)

        # Create clients in SDK client pool
        CollectionBase.create_clients_for_sdk_pool(self)

        # Load initial async_write docs into the cluster
        self.log.info("Initial doc generation process starting...")
        CollectionBase.load_data_from_spec_file(self, self.initial_data_spec,
                                                validate_docs=False)

    def failover_task(self):
        rest_nodes = self.cluster_util.get_nodes(self.cluster.master)
        self.log.info("servers to fail: {0}".format(self.servers_to_fail))
        if self.current_fo_strategy == CbServer.Failover.Type.GRACEFUL:
            self.rebalance_util.monitor_rebalance()
            for node in self.servers_to_fail:
                node = [t_node for t_node in rest_nodes if t_node.ip == node.ip][0]
                status = self.rest.perform_graceful_failover(node.id)
                if status is False:
                    self.fail("Graceful failover failed for %s" % node)
                self.sleep(5, "Wait for failover to start")
        elif self.current_fo_strategy == CbServer.Failover.Type.FORCEFUL:
            self.rebalance_util.monitor_rebalance()
            for node in self.servers_to_fail:
                node = [t_node for t_node in rest_nodes
                        if t_node.ip == node.ip][0]
                status = self.rest.perform_hard_failover(node.id)
                if status is False:
                    self.fail("Hard failover failed for %s" % node)
                self.sleep(5, "Wait for failover to start")

    def validate_hash_algo(self, username, expected_hash_algo):
        result, observed_hash_algo = self.security_util.get_hash_algo(self.cluster)
        if not result:
            self.fail("Failed to get password hash algorithm: {}".format(observed_hash_algo))

        if observed_hash_algo == expected_hash_algo:
            self.log.info("Hash algorithm validation success")
        else:
            self.fail("Hash algorith validation failed. Expected: {0} :: Observed: {1}".format(
                expected_hash_algo, observed_hash_algo))

    def wait_for_rebalance_to_start(self, rebalance_task):
        while rebalance_task.start_time is None:
            self.sleep(5, "Wait for rebalance to start")

    def test_password_hash_migration(self):
        """
        Test Password Hash Migration
        Step 1: set to SHA-1
        Step 2: create users
        Step 3: verify isasl.pw
        Step 4: set migration to true
        Step 5: set to argon2id
        Step 6: ns server authenticate
        Step 7: verify isasl.pw
        """
        self.log.info("Password Hash Migration test case")

        # set algo to SHA-1
        hash_algo_before_migration = "SHA-1"
        result, content = self.security_util.set_hash_algo(self.cluster, hash_algo_before_migration)
        if not result:
            self.fail("Failed to set password hash algorithm: {}".format(content))
        self.log.info("Successfully set password hash algorithm: {}".format(content))

        # create users
        user_name = "test_user"
        role = "admin"
        password = "password"
        payload = "name=" + user_name + "&roles=" + role + "&password=" + password
        security_apis = SecurityRestAPI(self.cluster)
        security_apis.create_local_user(user_name, payload)

        # verify in isasl.pw
        self.validate_hash_algo(user_name, hash_algo_before_migration)

        # set migration to true
        result, content = self.security_util.set_allow_hash_migration_during_auth(self.cluster)
        if not result:
            self.fail("Failed to set password hash migration: {}".format(content))
        self.log.info("Successfully set password hash migration: {}".format(content))

        # set algo to argon2id
        hash_algo_after_migration = "argon2id"
        result, content = self.security_util.set_hash_algo(self.cluster, hash_algo_after_migration)
        if not result:
            self.fail("Failed to set password hash algorithm: {}".format(content))
        self.log.info("Successfully set password hash algorithm: {}".format(content))

        # authenticate via ns server by hitting any random endpoint
        serverinfo_dict = {"ip": self.cluster.master.ip, "port": self.cluster.master.port,
                           "username": user_name, "password": password}
        rest_non_admin = RestConnection(serverinfo_dict)
        rest_non_admin.set_cluster_name("TEST_MIGRATION")

        # verify in isasl.pw
        self.validate_hash_algo(user_name, hash_algo_after_migration)

        # set algo back to SHA-1
        hash_algo_before_migration = "SHA-1"
        result, content = self.security_util.set_hash_algo(self.cluster, hash_algo_before_migration)
        if not result:
            self.fail("Failed to set password hash algorithm: {}".format(content))
        self.log.info("Successfully set password hash algorithm: {}".format(content))

    def test_password_hash_migration_rebalance(self):
        rebalance_task = None
        if self.rebalance_type == "rebalance_in":
            nodes_in = self.cluster.servers[
                       self.nodes_init:self.nodes_init + self.nodes_in]
            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       nodes_in, [],
                                                       retry_get_process_num=2000)

        elif self.rebalance_type == "swap_rebalance":
            nodes_in = self.cluster.servers[
                       self.nodes_init:self.nodes_init + self.nodes_in]
            nodes_out = self.cluster.servers[
                        1:self.nodes_out + 1]
            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       nodes_in, nodes_out,
                                                       retry_get_process_num=2000,
                                                       check_vbucket_shuffling=False)

        self.test_password_hash_migration()

        self.wait_for_rebalance_to_start(rebalance_task)
        self.sleep(5, "Wait after rebalance started")

        self.task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.fail("Failed to complete rebalance")

    def test_password_hash_migration_failover(self):
        try:
            self.failover_task()
        except Exception as e:
            failover_error = str(e)
            if failover_error:
                self.fail(failover_error)
        self.test_password_hash_migration()
        self.rebalance_util.monitor_rebalance()

    def test_password_hash_migration_node_down(self):
        # disable autofailover
        status = self.rest.update_auto_failover_settings(enabled="false",
                                                         timeout=60)
        self.assertTrue(status, "Auto-failover disable failed")
        for server_to_fail in self.servers_to_fail:
            self.cluster_util.stop_server(self.cluster, server_to_fail)
        self.sleep(60, "keeping the failure")
        self.test_password_hash_migration()
        self.log.info("Starting the server")
        for server_to_fail in self.servers_to_fail:
            self.cluster_util.start_server(self.cluster, server_to_fail)
        self.sleep(60, "Wait after server is back up!")
        self.test_password_hash_migration()

    def test_password_hash_migration_certificates(self):
        self.x509.generate_multiple_x509_certs(servers=self.cluster.servers)
        self.log.info("Manifest #########\n {0}".format(json.dumps(self.x509.manifest, indent=4)))
        for server in self.cluster.servers:
            _ = self.x509.upload_root_certs(server)
        self.x509.upload_node_certs(servers=self.cluster.servers)
        self.test_password_hash_migration()
