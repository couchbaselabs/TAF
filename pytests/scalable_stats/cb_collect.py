from random import sample
from threading import Thread

from bucket_collections.collections_base import CollectionBase
from cb_tools.cb_collectinfo import CbCollectInfo
from couchbase_helper.documentgenerator import doc_generator
from crash_test.constants import signum
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class CbCollectInfoTests(CollectionBase):
    def setUp(self):
        super(CbCollectInfoTests, self).setUp()

        self.log_setup_status("CbCollectInfo", "started")

        file_generic_name = "/tmp/cbcollect_info_test-"
        file_ext = ".zip"

        # Dict to store server-shell connections
        self.node_data = dict()
        for node in self.servers:
            shell = RemoteMachineShellConnection(node)
            self.node_data[node] = dict()
            self.node_data[node]["shell"] = shell
            self.node_data[node]["cb_collect"] = CbCollectInfo(shell)
            self.node_data[node]["cb_collect_result"] = dict()
            self.node_data[node]["cb_collect_file"] = \
                file_generic_name + node.ip.replace('.', '_') + file_ext

            # Remove cb_collect file (if exists)
            self.node_data[node]["shell"].execute_command(
                "rm -f %s" % self.node_data[node]["cb_collect_file"])

        self.snapshot_dir = \
            "/opt/couchbase/var/lib/couchbase/stats_data/snapshots"
        self.log_setup_status("CbCollectInfo", "complete")

    def tearDown(self):
        # Remove all cb_collect_info files and close shell connections
        for node in self.node_data:
            # Remove cb_collect file (if exists)
            self.node_data[node]["shell"].execute_command(
                "rm -f %s" % self.node_data[node]["cb_collect_file"])

            self.node_data[node]["shell"].disconnect()

        super(CbCollectInfoTests, self).tearDown()

    def __get_server_nodes(self, services="all"):
        nodes_in_cluster = list()
        for temp_node in self.cluster_util.get_nodes(self.cluster.master):
            for node in self.servers:
                if temp_node.ip == node.ip:
                    if services == "all":
                        nodes_in_cluster.append(node)
                    else:
                        for service in services:
                            if service in temp_node.services:
                                nodes_in_cluster.append(node)
                                break
                    break
        return nodes_in_cluster

    def __enable_diag_eval_on_non_local_hosts(self, cluster_nodes):
        for node in cluster_nodes:
            output, error = self.node_data[node]["shell"]\
                .enable_diag_eval_on_non_local_hosts()
            if output is not None:
                if "ok" not in output:
                    self.log.error(
                        "%s - Error enabling diag/eval on non-local hosts: %s"
                        % (node.ip, error))
            else:
                self.log.warning("Running in compatibility mode, "
                                 "not enabled diag/eval for non-local hosts")

    @staticmethod
    def __set_stat_setting(nodes, key, value):
        for node in nodes:
            RestConnection(node).diag_eval(
                "ns_config:set_sub(stats_settings, [{%s, %s}])" % (key, value))

    def test_cb_collect_info(self):
        """
        Run cb_collect_info to make sure it collects
        successfully on the cluster without any errors.
        (os_certify test)
        """
        nodes_in_cluster = self.__get_server_nodes()
        self.log.info("Starting cb-collection for nodes %s"
                      % [node.ip for node in nodes_in_cluster])
        for node in nodes_in_cluster:
            self.node_data[node]["cb_collect_task"] = Thread(
                target=self.cluster_util.run_cb_collect,
                args=[node, self.node_data[node]["shell"],
                      self.node_data[node]["cb_collect_file"]],
                kwargs={"options": "",
                        "result": self.node_data[node]["cb_collect_result"]})
            self.node_data[node]["cb_collect_task"].start()

        for node in nodes_in_cluster:
            try:
                t_node = self.node_data[node]
                t_node["cb_collect_task"].join(300)
                if str(t_node["cb_collect_result"]["file_size"]) == "0":
                    self.log_failure("%s - cbcollect file size is zero"
                                     % node.ip)
            except RuntimeError as e:
                self.log_failure("%s cbcollect_info timed-out: %s"
                                 % (node.ip, e))

        self.validate_test_failure()

    def test_with_server_stopped(self):
        """
        1. Disable auto-failover in the cluster
        2. Stop few servers on the cluster
        3. Run cb_collect_info on all nodes
        4. Make sure cb_collect works for stopped nodes as well
        """

        service_to_stop = self.input.param("affect_nodes_with_service",
                                           "kv").split(";")
        num_nodes_to_affect = self.input.param("num_nodes_to_affect", 1)

        nodes_in_cluster = self.__get_server_nodes()
        nodes_to_stop = sample(self.__get_server_nodes(service_to_stop),
                               num_nodes_to_affect)

        # Disable auto-failover to avoid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        self.log.info("Nodes to stop - %s" % nodes_to_stop)
        for node in nodes_to_stop:
            cb_error = CouchbaseError(self.log, self.node_data[node]["shell"])
            self.node_data[node]["cb_error"] = cb_error
            self.node_data[node]["cb_error"].create(CouchbaseError.STOP_SERVER)

        for node in nodes_in_cluster:
            self.node_data[node]["cb_collect_task"] = Thread(
                target=self.cluster_util.run_cb_collect,
                args=[node, self.node_data[node]["shell"],
                      self.node_data[node]["cb_collect_file"]],
                kwargs={"options": "",
                        "result": self.node_data[node]["cb_collect_result"]})
            self.node_data[node]["cb_collect_task"].start()

        for node in nodes_in_cluster:
            try:
                t_node = self.node_data[node]
                t_node["cb_collect_task"].join(300)
                if str(t_node["cb_collect_result"]["file_size"]) == "0":
                    self.log_failure("%s - cbcollect file size is zero"
                                     % node.ip)
            except RuntimeError as e:
                self.log_failure("%s cbcollect_info timed-out: %s"
                                 % (node.ip, e))

        # Restarting stopped nodes
        for node in nodes_to_stop:
            self.node_data[node]["cb_error"].revert(CouchbaseError.STOP_SERVER)

        self.bucket_util.is_warmup_complete(self.bucket_util.buckets)
        self.validate_test_failure()

    def test_with_process_crash(self):
        """
        1. Run cb_collect_info on selected nodes
        2. Kill target process on those nodes
        3. Make sure cb_collect works without any issues
        """
        processes_to_crash = self.input.param("crash_process",
                                              "memcached").split(";")
        num_nodes_to_affect = self.input.param("num_nodes_to_affect", 1)
        times_to_crash = self.input.param("times_to_crash", 1)
        nodes_in_cluster = self.__get_server_nodes()
        nodes_to_affect = sample(nodes_in_cluster, num_nodes_to_affect)

        # Start cb_collect_info on target nodes
        for node in nodes_to_affect:
            self.node_data[node]["cb_collect_task"] = Thread(
                target=self.cluster_util.run_cb_collect,
                args=[node, self.node_data[node]["shell"],
                      self.node_data[node]["cb_collect_file"]],
                kwargs={"options": "",
                        "result": self.node_data[node]["cb_collect_result"]})
            self.node_data[node]["cb_collect_task"].start()

        # Initiate process crash
        for itr in range(1, times_to_crash+1):
            self.log.info("Process crash itr :: %s" % itr)
            for node in nodes_to_affect:
                self.node_data[node]["shell"].kill_multiple_process(
                    processes_to_crash,
                    signum=signum["SIGKILL"])
            self.sleep(1)

        # Wait for cb_collect_thread to complete
        for node in nodes_to_affect:
            try:
                t_node = self.node_data[node]
                t_node["cb_collect_task"].join(300)
                if str(t_node["cb_collect_result"]["file_size"]) == "0":
                    self.log_failure("%s - cbcollect file size is zero"
                                     % node.ip)
            except RuntimeError as e:
                self.log_failure("%s cbcollect_info timed-out: %s"
                                 % (node.ip, e))

        self.validate_test_failure()

    def test_cb_collect_max_size_limit(self):
        """
        1. Simulate data such that it crosses max_limit supported by cb_collect
        2. Make sure the cb_collect logs does not crosses 'max_size_threshold'
        """
        total_index_to_create = 20
        def_bucket = self.bucket_util.buckets[0]
        load_gen = doc_generator(self.key, 0, 50000)
        nodes_in_cluster = self.__get_server_nodes()

        for bucket in self.bucket_util.buckets:
            self.sdk_client_pool.create_clients(bucket, [self.cluster.master])

        # Value in MB
        max_size_threshold = self.input.param("max_threshold", 1024)
        cluster_nodes = self.__get_server_nodes()
        self.__enable_diag_eval_on_non_local_hosts(cluster_nodes)
        self.__set_stat_setting(cluster_nodes,
                                "retention_size", max_size_threshold)
        # self.__set_stat_setting(cluster_nodes,
        #                         "average_sample_size", 100)
        # self.__set_stat_setting(cluster_nodes,
        #                         "prometheus_metrics_scrape_interval", 5)

        # Create required indexes on the bucket
        client = self.sdk_client_pool.get_client_for_bucket(def_bucket)
        client.cluster.query("CREATE PRIMARY INDEX default_primary on `%s`"
                             % def_bucket.name)
        index_names = list()
        for i in range(total_index_to_create):
            index_name = "index_%d" % i
            index_names.append(index_name)
            client.cluster.query(
                'CREATE INDEX `%s` on `%s`(name,age) '
                'WHERE age=%d'
                % (index_name, def_bucket.name, i))
        self.sdk_client_pool.release_client(client)

        doc_load_tasks = list()
        for bucket in self.bucket_util.buckets:
            task = self.task.async_continuous_doc_ops(
                self.cluster, bucket, load_gen,
                op_type="update",
                sdk_client_pool=self.sdk_client_pool,
                timeout_secs=60,
                process_concurrency=1)
            doc_load_tasks.append(task)

        self.sleep(60, "Wait before triggering cb_collect_info")

        # Start cb_collect_info on target nodes
        for node in nodes_in_cluster:
            self.node_data[node]["cb_collect_task"] = Thread(
                target=self.cluster_util.run_cb_collect,
                args=[node, self.node_data[node]["shell"],
                      self.node_data[node]["cb_collect_file"]],
                kwargs={"options": "",
                        "result": self.node_data[node]["cb_collect_result"]})
            self.node_data[node]["cb_collect_task"].start()

        # Wait for cb_collect_thread to complete
        for node in nodes_in_cluster:
            try:
                t_node = self.node_data[node]
                t_node["cb_collect_task"].join(300)
                if str(t_node["cb_collect_result"]["file_size"]) == "0":
                    self.log_failure("%s - cbcollect file size is zero"
                                     % node.ip)
            except RuntimeError as e:
                self.log_failure("%s cbcollect_info timed-out: %s"
                                 % (node.ip, e))

        for task in doc_load_tasks:
            task.end_task()
            self.task_manager.get_task_result(task)

        self.validate_test_failure()

    def test_snapshot_lifecycle_management(self):
        """
        """

    def test_periodic_snapshot_cleanup(self):
        """
        1. Simulate data such that it creates multiple stats snapshots
        2. Validate the created snapshots are cleanup after the configured
           time from the disk
        """
