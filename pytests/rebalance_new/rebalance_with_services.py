import time
from threading import Thread

from FtsLib.FtsOperations import FtsHelper
from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.rest_client import RestConnection
from rebalance_new.rebalance_base import RebalanceBaseTest
from sdk_client3 import SDKClient

from com.couchbase.client.core.error import InternalServerFailureException


class ServiceRebalanceTests(RebalanceBaseTest):
    def setUp(self):
        super(ServiceRebalanceTests, self).setUp()
        self.index_replica = self.input.param("index_replicas", 1)
        self.num_gsi_index = self.input.param("num_gsi_index", 50)
        self.num_fts_index = self.input.param("num_fts_index", 10)
        self.gsi_indexes_to_create_drop = \
            self.input.param("gsi_index_create_drop", 100)
        self.fts_indexes_to_create_drop = \
            self.input.param("fts_index_create_drop", 50)
        self.fts_index_partitions = self.input.param("fts_index_partition", 1)

        self.cluster_actions = \
            self.input.param("cluster_action", "rebalance_in").split(";")
        self.target_service_nodes = \
            self.input.param("target_nodes", "kv").split(";")
        self.num_nodes_to_run = self.input.param("num_nodes_to_run", 1)
        self.service_to_test = self.input.param("service_to_test", "2i")
        self.recovery_type = self.input.param("recovery_type", "delta")
        self.spare_node = self.servers[-1]

        self.action_call = dict()
        self.action_call["rebalance_in"] = self.perform_rebalance_in
        self.action_call["rebalance_out"] = self.perform_rebalance_out
        self.action_call["swap_rebalance"] = self.perform_swap_rebalance
        self.action_call["graceful_failover"] = self.perform_graceful_failover
        self.action_call["rebalance_failover_node"] = \
            self.perform_rebalance_out_failover_node
        self.action_call["add_back_failover_node"] = \
            self.perform_add_back_failover_node

    def tearDown(self):
        super(ServiceRebalanceTests, self).tearDown()

    def perform_rebalance_in(self, node):
        rest = RestConnection(self.cluster.master)
        nodes_in_server = self.cluster_util.get_nodes_in_cluster()
        self.task.rebalance(nodes_in_server, [node], [])
        self.sleep(30, "Wait for rebalance to start")
        self.assertTrue(rest.monitorRebalance(stop_if_loop=True),
                        "Rebalance_in failed")

    def perform_rebalance_out(self, node):
        rest = RestConnection(self.cluster.master)
        nodes_in_server = self.cluster_util.get_nodes_in_cluster()
        self.task.rebalance(nodes_in_server, [], [node])
        self.sleep(30, "Wait for rebalance to start")
        self.assertTrue(rest.monitorRebalance(stop_if_loop=True),
                        "Rebalance_out failed")
        for t_node in nodes_in_server:
            cluster_node = t_node
            if cluster_node.ip != self.cluster.master.ip:
                self.cluster.update_master(cluster_node)
                break

    def perform_swap_rebalance(self, node):
        rest = RestConnection(self.cluster.master)
        self.task.rebalance(self.cluster_util.get_nodes_in_cluster(),
                            [self.spare_node], [node],
                            services=None,
                            check_vbucket_shuffling=False)
        self.sleep(30, "Wait for rebalance to start")
        self.assertTrue(rest.monitorRebalance(stop_if_loop=True),
                        "Swap_rebalance failed")
        if node.ip == self.cluster.master.ip:
            self.cluster.master = self.spare_node
        self.spare_node = node
        self.cluster.update_master()

    def perform_graceful_failover(self, node):
        rest = None
        nodes_in_cluster = self.cluster_util.get_nodes_in_cluster()
        for t_node in nodes_in_cluster:
            if t_node.ip != node.ip:
                rest = RestConnection(t_node)
                self.new_master = t_node
                break
        rest.fail_over("ns_1@"+node.ip, graceful=True)
        self.sleep(10, "Wait for failover to start")
        self.assertTrue(rest.monitorRebalance(stop_if_loop=True),
                        "Failover failed for node %s" % node.ip)

    def perform_rebalance_out_failover_node(self, node):
        current_nodes = list()
        for t_node in self.cluster_util.get_nodes_in_cluster():
            if t_node.ip != node.ip:
                current_nodes.append(t_node)

        # Rebalance_out failed-over node
        rest = RestConnection(self.cluster.master)
        self.task.rebalance(current_nodes, [], [])
        self.sleep(10, "Wait after cluster rebalance")
        self.assertTrue(rest.monitorRebalance(stop_if_loop=True),
                        "Rebalance failed with failover node %s" % node.ip)
        self.cluster.update_master(self.new_master)

    def perform_add_back_failover_node(self, node):
        rest = RestConnection(self.cluster.master)
        rest.set_recovery_type("ns_1@"+node.ip, self.recovery_type)
        self.task.rebalance(self.cluster_util.get_nodes_in_cluster(), [], [])
        self.sleep(30)
        self.assertTrue(rest.monitorRebalance(stop_if_loop=True),
                        "Rebalance failed with failover node %s" % node.ip)
        self.cluster.update_master(self.new_master)

    def load_gsi_fts_indexes(self):
        def create_gsi_index(b_name, t_index):
            query = gsi_create_template % (b_name.replace(".", ""), t_index,
                                           b_name, self.index_replica)
            self.log.debug("Executing query: %s" % query)
            try:
                n1ql_helper.run_cbq_query(query)
            except Exception as e:
                self.log.critical(e)

        def drop_gsi_index(b_name, t_index):
            query = gsi_drop_template % (b_name, b_name.replace(".", ""),
                                         t_index)
            self.log.debug("Executing query: %s" % query)
            try:
                n1ql_helper.run_cbq_query(query)
            except Exception as e:
                self.log.critical(e)

        def create_fts_index(b_name, t_index):
            fts_index_name = "%s_fts_%d" % (b_name.replace(".", ""), t_index)
            status, content = fts_helper.create_fts_index_from_json(
                fts_index_name,
                fts_param_template % (fts_index_name, b_name,
                                      self.fts_index_partitions))
            if status is False:
                self.fail("Failed to create fts index %s: %s"
                          % (fts_index_name, content))

        def drop_fts_index(b_name, t_index):
            fts_index_name = "%s_fts_%d" % (b_name.replace(".", ""), t_index)
            status, content = fts_helper.delete_fts_index(fts_index_name)
            if status is False:
                self.fail("Failed to drop fts index %s: %s"
                          % (fts_index_name, content))

        n1ql_node = self.cluster.query_nodes[0]
        fts_helper = FtsHelper(self.cluster.fts_nodes[0])
        n1ql_helper = N1QLHelper(server=n1ql_node, use_rest=True, log=self.log)

        gsi_index_name_pattern = "%s_primary_%d"
        gsi_create_template = "CREATE PRIMARY INDEX `" \
                              + gsi_index_name_pattern \
                              + "` on `%s` USING GSI " \
                              + "WITH {\"num_replica\": %s}"
        gsi_drop_template = "DROP INDEX `%s`.`" + gsi_index_name_pattern \
                            + "` USING GSI"
        fts_param_template = '{ \
            "type": "fulltext-index", \
            "name": "%s", \
            "sourceType": "couchbase", \
            "sourceName": "%s", \
            "planParams": { \
              "maxPartitionsPerPIndex": 171, \
              "indexPartitions": %d \
            }, \
            "params": { \
              "doc_config": { \
                "docid_prefix_delim": "", \
                "docid_regexp": "", \
                "mode": "type_field", \
                "type_field": "type" \
              }, \
              "mapping": { \
                "analysis": {}, \
                "default_analyzer": "standard", \
                "default_datetime_parser": "dateTimeOptional", \
                "default_field": "_all", \
                "default_mapping": { \
                  "dynamic": false, \
                  "enabled": true, \
                  "properties": { \
                    "rsx": { \
                      "dynamic": false, \
                      "enabled": true, \
                      "fields": [ \
                        { \
                          "index": true, \
                          "name": "rsx", \
                          "type": "text" \
                        } \
                      ] \
                    } \
                  } \
                }, \
                "default_type": "_default", \
                "docvalues_dynamic": true, \
                "index_dynamic": true, \
                "store_dynamic": false, \
                "type_field": "_type" \
              }, \
              "store": { \
                "indexType": "scorch" \
              } \
            }, \
            "sourceParams": {} \
        }'

        # Open SDK for connection for running n1ql queries
        client = SDKClient([self.cluster.master], self.bucket_util.buckets[0])

        for bucket in self.bucket_util.buckets[:4]:
            self.log.info("Creating GSI indexes %d::%d for %s"
                          % (0, self.num_gsi_index, bucket.name))
            for index in range(0, self.num_gsi_index):
                create_gsi_index(bucket.name, index)
            self.log.info("Done creating GSI indexes for %s" % bucket.name)

        for bucket in self.bucket_util.buckets[:3]:
            self.log.info("Creating FTS indexes %d::%d for %s"
                          % (0, self.num_fts_index, bucket.name))
            for index in range(0, self.num_fts_index):
                create_fts_index(bucket.name, index)
            self.log.info("Done creating FTS indexes for %s" % bucket.name)

        for bucket in self.bucket_util.buckets[:4]:
            self.log.info("Create and drop %s GSI indexes on %s"
                          % (self.gsi_indexes_to_create_drop, bucket.name))
            for index in range(self.num_gsi_index,
                               self.num_gsi_index
                               + self.gsi_indexes_to_create_drop):
                create_gsi_index(bucket.name, index)
                drop_gsi_index(bucket.name, index)

        for bucket in self.bucket_util.buckets[:3]:
            self.log.info("Create and drop %s FTS indexes on %s"
                          % (self.fts_indexes_to_create_drop, bucket.name))
            for index in range(self.num_fts_index,
                               self.num_fts_index
                               + self.fts_indexes_to_create_drop):
                create_fts_index(bucket.name, index)
                drop_fts_index(bucket.name, index)

        # Close the SDK connection
        client.close()

    def test_MB_43291(self):
        """
        1. Cluster with 3 kv, 2 index+n1ql, 4 search nodes
        2. 6 buckets with 5000 docs each
        3. Built 200 GSI indexes with replica 1 (50 indexes on each bucket)
        4. Created 30 fts custom indexes (10 indexes on 3 buckets),
           just to add more entries to meta-kv
        5. Create and Drop 100 gsi indexes sequentially on 4 buckets
           (so this would be adding more entries of create/drop of 400 indexes)
        6. Create and drop 50 fts indexes on 3 buckets.
        7. Graceful Failover a node with kv service.
        8. Failover took 1hr 30 minutes
        """

        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.load_gsi_fts_indexes()

        nodes_involved = list()
        for service in self.target_service_nodes:
            num_nodes_run = 0
            if service == "kv":
                nodes_to_play = self.cluster.kv_nodes
            elif service == "index":
                nodes_to_play = self.cluster.index_nodes
            elif service == "n1ql":
                nodes_to_play = self.cluster.query_nodes
            elif service == "2i":
                nodes_to_play = self.cluster.cbas_nodes
            elif service == "fts":
                nodes_to_play = self.cluster.fts_nodes
            elif service == "eventing":
                nodes_to_play = self.cluster.eventing_nodes
            else:
                self.fail("Invalid service %s" % service)

            for node in nodes_to_play:
                if node.ip in nodes_involved:
                    continue

                # Note affected nodes to avoid repeating action on same node
                # Happens when >1 service running on the node
                self.log.info("Master node: %s" % self.cluster.master.ip)
                nodes_involved.append(node.ip)

                for cluster_action in self.cluster_actions:
                    self.log.info("Performing '%s' on node %s"
                                  % (cluster_action, node.ip))
                    self.action_call[cluster_action](node)

                # Break if max nodes to run has reached per service
                num_nodes_run += 1
                if num_nodes_run >= self.num_nodes_to_run:
                    break

