import copy

from basetestcase import BaseTestCase
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection


class RebalanceBaseTest(BaseTestCase):
    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.doc_ops = self.input.param("doc_ops", "create")
        self.doc_size = self.input.param("doc_size", 10)
        self.key_size = self.input.param("key_size", 0)
        self.zone = self.input.param("zone", 1)
        self.default_view_name = "default_view"
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)
        self.max_verify = self.input.param("max_verify", None)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
        self.key = 'test_docs'.rjust(self.key_size, '0')
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket(replica=self.num_replicas)
        self.bucket_util.create_default_bucket()
        self.bucket_util.add_rbac_user()

        gen_create = self.get_doc_generator(0, self.num_items)
        self.print_cluster_stat_task = self.cluster_util.async_print_cluster_stats()
        for bucket in self.bucket_util.buckets:
            print_ops_task = self.bucket_util.async_print_bucket_ops(bucket)
            task = self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "create", 0,
                                                 persist_to=self.persist_to, replicate_to=self.replicate_to,
                                                 batch_size=10, timeout_secs=self.sdk_timeout,
                                                 process_concurrency=8, retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)
            print_ops_task.end_task()
            self.task_manager.get_task_result(print_ops_task)
        self.gen_load = self.get_doc_generator(0, self.num_items)
        # gen_update is used for doing mutation for 1/2th of uploaded data
        self.gen_update = self.get_doc_generator(0, (self.num_items / 2 - 1))
        self.log.info("==========Finished rebalance base setup========")

    def tearDown(self):
        if hasattr(self, "print_cluster_stat_task") and self.print_cluster_stat_task:
            self.print_cluster_stat_task.end_task()
            self.task_manager.get_task_result(self.print_cluster_stat_task)
        self.cluster_util.print_cluster_stats()
        super(RebalanceBaseTest, self).tearDown()

    def shuffle_nodes_between_zones_and_rebalance(self, to_remove=None):
        """
        Shuffle the nodes present in the cluster if zone > 1.
        Rebalance the nodes in the end.
        Nodes are divided into groups iteratively. i.e: 1st node in Group 1,
        2nd in Group 2, 3rd in Group 1 & so on, when zone=2
        :param to_remove: List of nodes to be removed.
        """
        if not to_remove:
            to_remove = []
        serverinfo = self.servers[0]
        rest = RestConnection(serverinfo)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [serverinfo.ip]}
        # Create zones, if not existing, based on params zone in test.
        # Shuffle the nodes between zones.
        if int(self.zone) > 1:
            for i in range(1, int(self.zone)):
                a = "Group "
                zones.append(a + str(i + 1))
                if not rest.is_zone_exist(zones[i]):
                    rest.add_zone(zones[i])
                nodes_in_zone[zones[i]] = []
            # Divide the nodes between zones.
            nodes_in_cluster = [node.ip for node in self.cluster_util.get_nodes_in_cluster()]
            nodes_to_remove = [node.ip for node in to_remove]
            for i in range(1, len(self.servers)):
                if self.servers[i].ip in nodes_in_cluster and self.servers[i].ip not in nodes_to_remove:
                    server_group = i % int(self.zone)
                    nodes_in_zone[zones[server_group]].append(self.servers[i].ip)
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = list(set(nodes_in_zone[zones[i]]) -
                                    set([node for node in rest.get_nodes_in_zone(zones[i])]))
                rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[i])
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses() if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started = rest.rebalance(otpNodes=otpnodes, ejectedNodes=nodes_to_remove)
        if started:
            result = rest.monitorRebalance()
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        # Verify replicas of one node should not be in the same zone as active vbuckets of the node.
        if self.zone > 1:
            self.cluster_util.verify_replica_distribution_in_zones(nodes_in_zone)

    def add_remove_servers_and_rebalance(self, to_add, to_remove):
        """
        Add and/or remove servers and rebalance.
        :param to_add: List of nodes to be added.
        :param to_remove: List of nodes to be removed.
        """
        serverinfo = self.servers[0]
        rest = RestConnection(serverinfo)
        for node in to_add:
            rest.add_node(user=serverinfo.rest_username, password=serverinfo.rest_password,
                          remoteIp=node.ip)
        self.shuffle_nodes_between_zones_and_rebalance(to_remove)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster + to_add) - set(to_remove))

    def add_remove_servers(self, servers=[], list=[], remove_list=[], add_list=[]):
        """ Add or Remove servers from server list """
        initial_list = copy.deepcopy(list)
        for add_server in add_list:
            for server in self.servers:
                if add_server is not None and server.ip == add_server.ip:
                    initial_list.append(add_server)
        for remove_server in remove_list:
            for server in initial_list:
                if remove_server is not None and server.ip == remove_server.ip:
                    initial_list.remove(server)
        return initial_list

    def get_doc_generator(self, start, end):
        age = range(5)
        first = ['james', 'sharon']
        body = [''.rjust(self.doc_size - 10, 'a')]
        template = '{{ "age": {0}, "first_name": "{1}", "body": "{2}"}}'
        generator = DocumentGenerator(self.key, template, age, first, body,
                                      start=start, end=end)
        return generator

    def _load_all_buckets(self, kv_gen, op_type, exp, flag=0,
                          only_store_hash=True, batch_size=1000, pause_secs=1,
                          timeout_secs=30, compression=True):
        tasks = self.bucket_util._async_load_all_buckets(self.cluster, kv_gen,
                                                         op_type, exp, flag,
                                                         self.persist_to,
                                                         self.replicate_to,
                                                         only_store_hash,
                                                         batch_size, pause_secs,
                                                         timeout_secs, compression)
        for task in tasks:
            self.task_manager.get_task_result(task)
