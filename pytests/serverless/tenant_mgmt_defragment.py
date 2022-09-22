from pytests.serverless.serverless_onprem_basetest import \
    ServerlessOnPremBaseTest
from membase.api.rest_client import RestConnection
from Cb_constants import CbServer
from BucketLib.bucket import Bucket
from pytests.bucket_collections.collections_base import CollectionBase


class TenantManagementOnPremDefragment(ServerlessOnPremBaseTest):
    def setUp(self):
        super(TenantManagementOnPremDefragment, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.nodes_in = self.input.param("nodes_in", 0)
        self.nodes_out = self.input.param("nodes_out", 0)
        self.data_spec_name = self.input.param("data_spec_name", None)
        self.extra_buckets = self.input.param("extra_buckets", 30)
        self.spec_name = self.input.param("bucket_spec", None)
        self.extra_bucket_weight = self.input.param("extra_bucket_weight", 1)
        self.extra_bucket_width = self.input.param("extra_bucket_width", 1)
        self.expect_space_warning = self.input.param("expect_space_warning",
                                                  False)
        if self.spec_name:
            CollectionBase.deploy_buckets_from_spec_file(self)

    def tearDown(self):
        super(TenantManagementOnPremDefragment, self).tearDown()

    def data_load(self):
        doc_loading_spec_name = "initial_load"
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(
            doc_loading_spec_name)
        task = self.bucket_util.run_scenario_from_spec(self.task,
                                                       self.cluster,
                                                       self.cluster.buckets,
                                                       doc_loading_spec,
                                                       mutation_num=0,
                                                       async_load=True)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.validate_doc_loading_results(task)
        if task.result is False:
            raise Exception("doc load/verification failed")

    def verify_de_fragmentation(self, target_node):
        util_map_post_rebal = self.rest.get_utilisation_map()
        for server in util_map_post_rebal.keys():
            self.assertTrue(util_map_post_rebal[server]["weight"] > 0,
                            "Some nodes not getting utilised")
            if str(target_node["server"].ip) in str(server):
                self.assertTrue(util_map_post_rebal[server]["weight"] <
                                target_node["weight"],
                                "overall weight not decreased for target node")

    def create_serverless_bucket(self, name_prefix="bucket_", num=2,
                                 bucket_weight=1, bucket_width=1):
        def __get_bucket_params(b_name, ram_quota=256, width=1, weight=1):
            self.log.debug("Creating bucket param")
            return {
                Bucket.name: b_name,
                Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
                Bucket.ramQuotaMB: ram_quota,
                Bucket.storageBackend: Bucket.StorageBackend.magma,
                Bucket.width: width,
                Bucket.weight: weight
            }
        for i in range(num):
            name = name_prefix + str(i)
            bucket_params = __get_bucket_params(
                b_name=name, weight=bucket_weight, width=bucket_width)
            bucket_obj = Bucket(bucket_params)
            try:
                self.bucket_util.create_bucket(self.cluster, bucket_obj,
                                               wait_for_warmup=True)
            except Exception as e:
                raise e

    def create_sdk_clients(self):
        CollectionBase.create_sdk_clients(
            self.task_manager.number_of_threads,
            self.cluster.master,
            self.cluster.buckets,
            self.sdk_client_pool,
            self.sdk_compression)

    def get_zone_bucket_map(self):
        node_zone_bucket = dict()
        zone_map = dict()
        for zone in self.rest.get_zone_names():
            zone_map[zone] = self.rest.get_nodes_in_zone(zone)
        for bucket in self.cluster.buckets:
            for server in bucket.servers:
                if server not in node_zone_bucket:
                    node_zone_bucket[server] = dict()
                    node_zone_bucket[server]["buckets"] = [bucket]
                else:
                    node_zone_bucket[server]["buckets"].append(bucket)
                for zone in zone_map.keys():
                    if server.ip in zone_map[zone]:
                        node_zone_bucket[server]["zone"] = zone
        return node_zone_bucket, zone_map

    def fragmentation_scenario(self, extra_buckets=30,
                               extra_bucket_weight=1, extra_bucket_width=1,
                               expect_space_warning=False):
        """
        expands all bucket weights residing single node
        add more buckets to cluster to cause fragmentation
        """
        node_stat = self.rest.get_nodes_self()
        max_weight = node_stat.limits[CbServer.Services.KV]["weight"]
        max_weight = (max_weight / 3)
        target_node = dict()
        nodes_of_buckets = dict()
        server_bucket_map = dict()
        for bucket in self.cluster.buckets:
            for server in bucket.servers:
                if server not in server_bucket_map:
                    server_bucket_map[server] = [bucket]
                else:
                    server_bucket_map[server].append(bucket)

        node_weight = 0
        for server in server_bucket_map.keys():
            target_node["server"] = server
            for buckets in server_bucket_map[server]:
                nodes_of_buckets[buckets] = buckets.servers
                self.bucket_util.update_bucket_property(
                    self.cluster.master, buckets, bucket_weight=max_weight)
                node_weight += max_weight
                buckets.serverless.weight = max_weight
            break
        target_node["weight"] = node_weight
        validation = self.bucket_util.validate_serverless_buckets(
            self.cluster, self.cluster.buckets)
        self.assertTrue(validation, "Bucket validation failed")

        for i in range(extra_buckets):
            stat = self.rest.cluster_status()
            low_space_warning = False
            for node in stat["nodes"]:
                if "error" in node["defragmented"]["kv"]:
                    self.assertTrue(expect_space_warning,
                                    "Space warning in nodes was not expected")
                    low_space_warning = True
                    break
            if low_space_warning:
                break
            try:
                self.create_serverless_bucket("extra_bucket" + str(i), num=1,
                                              bucket_weight=extra_bucket_weight,
                                              bucket_width=extra_bucket_width)
            except Exception as e:
                self.log.error("Exception occurred: %s" % str(e))
                self.assertFalse(expect_space_warning,
                                 "Space warning in nodes was expected")
                break
        return target_node

    def test_de_fragment_rebalance(self):
        nodes_in = self.cluster.servers[
                   self.nodes_init:self.nodes_init + self.nodes_in]
        defrag_option = dict()
        self.create_serverless_bucket("test_bucket",
                                      self.nodes_init
                                      - len(self.cluster.buckets), 1, 1)
        self.create_sdk_clients()
        self.data_load()
        target_node = self.fragmentation_scenario(self.extra_buckets,
                                                  self.extra_bucket_weight,
                                                  self.extra_bucket_width,
                                                  self.expect_space_warning)
        zone_bucket_map, zone_map = self.get_zone_bucket_map()

        add_to_nodes = dict()
        defrag_option["defragmentZones"] = []
        defrag_option["knownNodes"] = [nodes.ip for nodes in
                                       zone_bucket_map.keys()]
        for node in nodes_in:
            defrag_option["knownNodes"].append(node.ip)
        for zone in zone_map.keys():
            defrag_option["defragmentZones"].append(zone)
            add_to_nodes[zone] = self.nodes_in/3
        rebalance_task = self.task.async_rebalance(self.cluster, nodes_in, [],
                                                   retry_get_process_num=3000,
                                                   add_nodes_server_groups=
                                                   add_to_nodes,
                                                   defrag_options=defrag_option)
        self.task_manager.get_task_result(rebalance_task)

        # checking cluster balanced after de-frag re-balance
        if rebalance_task.result is False:
            self.fail("Rebalance failed")
        zone_bucket_map_post_rebalance, zone_map_post_rebalance = \
            self.get_zone_bucket_map()

        # checking if weighted buckets are rebalanced
        self.assertTrue(
            len(set(zone_bucket_map[target_node["server"]]["buckets"]).
                intersection(
                set(zone_bucket_map_post_rebalance[target_node["server"]][
                        "buckets"]))) < len(
                zone_bucket_map[target_node["server"]]["buckets"])
            , "target bucket nodes not changed")

        # verify target weight after re-balance
        self.verify_de_fragmentation(target_node)




