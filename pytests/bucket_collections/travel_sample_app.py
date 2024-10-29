import yaml
from random import choice, sample

from bucket_collections.app.app_basetest import AppBase
from bucket_collections.app.constants import global_vars
from bucket_collections.app.lib import query_util
from bucket_collections.app.scenarios.airline import Airline
from bucket_collections.app.scenarios.cluster import Cluster
from bucket_collections.app.scenarios.guest import Guest
from bucket_collections.app.scenarios.hotel import Hotel
from bucket_collections.app.scenarios.user import User
from cb_constants import DocLoading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from couchbase_helper.documentgenerator import doc_generator
from py_constants import CbServer
from rebalance_utils.rebalance_util import RebalanceUtil


class TravelSampleApp(AppBase):
    def setUp(self):
        super(TravelSampleApp, self).setUp()

        self.log_setup_status("TravelSampleApp", "started")
        self.monitor_ops_rate = self.input.param("monitor_ops_rate", False)
        self.playbook = self.input.param("playbook", "steady_state")

        # Split wrt '-', indicating per node,
        # per node service defined using the delimiter ';'
        self.reb_in_services = self.input.param("reb_in_services",
                                                "").split("-")
        self.reb_out_services = self.input.param("reb_out_services",
                                                 "").split("-")

        self.activities = list()
        # Start monitoring doc_ops
        if self.monitor_ops_rate:
            self.bucket.stats.manage_task("start", self.task_manager,
                                          cluster=self.cluster,
                                          bucket=self.bucket,
                                          monitor_stats=["doc_ops"],
                                          sleep=1)
        # Fetch all tenants from the bucket (Scope will collection "meta_data")
        self.tenants = list()
        travel_sample_bucket = self.bucket_util.get_bucket_obj(
            self.cluster.buckets, "travel-sample")
        for scope_name, scope in travel_sample_bucket.scopes.items():
            for c_name, _ in scope.collections.items():
                if c_name == "meta_data":
                    self.tenants.append(scope_name)
                    break

        if self.initial_load:
            self.__load_initial_data()

        self.app_iteration = self.input.param("iteration", 1)
        if self.tenants:
            global_vars.app_current_date = \
                query_util.CommonUtil.get_current_date(self.tenants[0])

        with open(self.app_path + "/scenarios/" + self.playbook + ".yaml",
                  "r") as fp:
            self.activities = yaml.safe_load(fp.read())["activities"]
        self.log_setup_status("TravelSampleApp", "complete")

    def tearDown(self):
        # Stop monitoring doc_ops
        if self.monitor_ops_rate:
            self.bucket.stats.manage_task("stop", self.task_manager,
                                          cluster=self.cluster,
                                          bucket=self.bucket,
                                          monitor_stats=["doc_ops"],
                                          sleep=1)

        # Start tearDown process
        super(TravelSampleApp, self).tearDown()

    def __load_initial_data(self):
        load_tasks = list()
        doc_gen = doc_generator(self.key, 0, self.num_items)
        for bucket in self.cluster.buckets:
            self.log.info(f"Loading data into '{bucket.name}' bucket")
            if bucket.name == "travel-sample":
                # Create collection meta_data document
                sdk_client = self.sdk_clients["bucket_data_writer"]
                for tenant in self.tenants:
                    sdk_client.select_collection(scope_name=tenant,
                                                 collection_name="meta_data")
                    app_data = {"date": "2001-01-01"}
                    result = sdk_client.crud(DocLoading.Bucket.DocOps.CREATE,
                                             "application", app_data)
                    self.assertTrue(result["status"],
                                    "App_meta creation failed")
                    bucket.scopes[self.tenants[0]].collections["meta_data"] \
                        .num_items += 1

                    create_users = User(bucket,
                                        scope=tenant,
                                        op_type="scenario_user_registration",
                                        num_items=20000)
                    create_users.start()
                    create_users.join()
            else:
                load_tasks.append(self.task.async_load_gen_docs(
                    self.cluster, bucket, doc_gen,
                    DocLoading.Bucket.DocOps.CREATE))

        for load_task in load_tasks:
            self.task_manager.get_task_result(load_task)

    def __travel_sample_app_ops(self, op_type):
        if op_type == "start":
            # Start app related tasks for doc / query ops
            self.user_task = User(self.bucket, choice(self.tenants),
                                  op_type="random", op_count=9999999)
            self.guest_task = Guest(self.bucket, choice(self.tenants),
                                    op_type="random", op_count=9999999)
            self.hotel_task = Hotel(self.bucket, choice(self.tenants),
                                    op_type="random", op_count=9999999)
            self.airline_task = Airline(self.bucket, choice(self.tenants),
                                        op_type="random", op_count=9999999)
            for task in [self.user_task, self.guest_task, self.hotel_task,
                         self.airline_task]:
                task.start()
        elif op_type == "stop":
            for task in [self.user_task, self.guest_task, self.hotel_task,
                         self.airline_task]:
                task.stop_operation = True
                task.join(120)
        else:
            self.fail(f"Invalid operation: {op_type}")

    @staticmethod
    def get_reb_topology_dict():
        return {
            CbServer.Services.INDEX: list(),
            CbServer.Services.N1QL: list(),
            CbServer.Services.EVENTING: list(),
            CbServer.Services.CBAS: list(),
            CbServer.Services.BACKUP: list(),
            CbServer.Services.FTS: list(),
        }

    @staticmethod
    def __get_nodes_with_service(nodes, service_name):
        result = list()
        for node in nodes:
            if service_name in node.services:
                result.append(node)
        return result

    def __set_nodes_with_desired_cluster_config(self, req_service_config):
        def get_otp_id_for_server(all_nodes, server, refresh_nodes=False):
            if refresh_nodes:
                all_nodes = self.cluster_util.get_nodes(self.cluster.master,
                                                        inactive_added=True)
            for t_node in all_nodes:
                if t_node.ip == server.ip:
                    return t_node.id

        self.log.info("Setting up cluster to desired state")
        reb_topology = self.get_reb_topology_dict()
        self.cluster_util.update_cluster_nodes_service_list(
            self.cluster, inactive_added=True)
        nodes = self.cluster_util.get_nodes(self.cluster.master,
                                            inactive_added=True)
        cluster_nodes_without_kv = list()
        cluster_nodes_with_kv = list()
        for node in nodes:
            if "kv" in node.services:
                cluster_nodes_with_kv.append(node)
            else:
                cluster_nodes_without_kv.append(node)

        kv_node_index = 0
        unused_node_index = 0
        req_serviceless_nodes = 0

        for services in req_service_config:
            if CbServer.Services.KV in services:
                if kv_node_index == len(cluster_nodes_with_kv):
                    # We don't have any more KV nodes to handle this,
                    # so rebalance-in a new node from free servers
                    for t_server in self.cluster.servers:
                        if t_server not in self.cluster.nodes_in_cluster:
                            # Found a unused node
                            self.log.info(f"Add {t_server.ip} with {services}")
                            self.cluster_util.add_node(
                                self.cluster, t_server,
                                services=",".join(services), rebalance=False)

                            # Add this to reb_topology, so we won't reset the
                            # services by mistake during final rebalance
                            for service in services:
                                if service != CbServer.Services.KV:
                                    otp_node = get_otp_id_for_server(
                                        nodes, t_server, refresh_nodes=True)
                                    reb_topology[service].append(otp_node)
                            break
                    else:
                        self.fail("Unable to add new node into the cluster")
                    continue
                # Else add this service as part of the existing KV node
                for service in services:
                    if service == CbServer.Services.KV:
                        continue
                    otp_node = get_otp_id_for_server(
                        nodes, cluster_nodes_with_kv[kv_node_index],
                        refresh_nodes=True)
                    reb_topology[service].append(otp_node)
                # Increment kv_index to avoid reusing the same node
                kv_node_index += 1
            elif services:
                if unused_node_index == len(cluster_nodes_without_kv):
                    # We don't have any more nodes to handle this,
                    # so rebalance-in a new node from free servers
                    for t_server in self.cluster.servers:
                        if t_server not in self.cluster.nodes_in_cluster:
                            # Found a unused node
                            self.log.info(f"Add {t_server.ip} with {services}")
                            self.cluster_util.add_node(
                                self.cluster, t_server,
                                services=",".join(services), rebalance=False)
                            # Add this to reb_topology, so we won't reset the
                            # services by mistake during final rebalance
                            for service in services:
                                if service != CbServer.Services.KV:
                                    otp_node = get_otp_id_for_server(
                                        nodes, t_server, refresh_nodes=True)
                                    reb_topology[service].append(otp_node)
                            break
                    else:
                        self.fail("Unable to add new node into the cluster")
                    continue
                # Else take a next node and add services as per req. config
                for service in services:
                    otp_node = get_otp_id_for_server(
                        nodes, cluster_nodes_without_kv[unused_node_index])
                    reb_topology[service].append(otp_node)
                # Increment non_kv_index to avoid reusing the same node
                unused_node_index += 1
            else:
                # Need a service-less node
                req_serviceless_nodes += 1

        eject_nodes = cluster_nodes_without_kv[unused_node_index:]
        if req_serviceless_nodes > 0:
            eject_nodes = eject_nodes[:-req_serviceless_nodes]

        self.log.info(f"Reb topology: {reb_topology}")
        result = self.task.rebalance(self.cluster,
                                     to_add=[], to_remove=eject_nodes,
                                     service_topology=reb_topology)
        self.assertTrue(result, "Rebalance failed")

    def __perform_fo(self, cluster_node, fo_node,
                     reb_topology, nodes_considered,
                     fo_type,
                     recovery_type=None,
                     services_in_add_back_node=None):
        rest = ClusterRestAPI(cluster_node)
        reb_util = RebalanceUtil(cluster_node)
        if fo_type == "graceful":
            fo_method = rest.perform_graceful_failover
        elif fo_type == "hard":
            fo_method = rest.perform_hard_failover
        else:
            self.fail(f"Invalid fo_type={fo_type}")

        self.log.info(f"{fo_node.ip} - {fo_type} failover")
        ok, _ = fo_method(fo_node.id)
        self.assertTrue(ok,"Failover trigger failed")
        ok = reb_util.monitor_rebalance()
        self.assertTrue(ok,"Failover failure")

        # Append to the list to make sure, we won't use the node again
        nodes_considered.append(fo_node)

        # Populate recovery map to make sure service map gets updated
        if recovery_type is not None:
            self.log.info(f"{fo_node.ip} - recovery={recovery_type}")
            rest.set_recovery_type(fo_node.id, recovery_type)
            for service in services_in_add_back_node:
                reb_topology[service].append(fo_node.id)

    def trigger_remap_service_rebalance(self, remap_service_conf):
        valid_action_str = [
            "reb_in",
            "reb_out",
            "graceful_fo_delta_recovery",
            "graceful_fo_full_recovery",
            "graceful_fo_reb_out",
            "hard_fo_delta_recovery",
            "hard_fo_full_recovery",
            "hard_fo_reb_out"
        ]
        rest = ClusterRestAPI(self.cluster.master)
        nodes = self.cluster_util.get_nodes(self.cluster.master,
                                            inactive_added=True)
        nodes_considered = list()
        reb_topology = self.get_reb_topology_dict()
        spare_nodes = self.cluster_util.get_spare_nodes(self.cluster)
        spare_node_index = 0
        for (src_services, services_or_action) in remap_service_conf:
            # To sort and simplify comparison
            src_services = set(src_services)
            if services_or_action and services_or_action[0] == "reb_in":
                self.log.info(f"Adding node: {spare_nodes[spare_node_index]}")
                # Rebalance_in is a spl case, where we can blindly add a node
                # without checking existing nodes / services
                otp_node = self.cluster_util.add_node(
                    self.cluster,
                    node=spare_nodes[spare_node_index],
                    services=services_or_action[1],
                    rebalance=False)
                for service in services_or_action[1]:
                    reb_topology[service].append(otp_node)
            else:
                for node in nodes:
                    if set(node.services) == src_services \
                            and node not in nodes_considered:
                        if services_or_action[0] in valid_action_str:
                            if services_or_action[0] == "reb_out":
                                self.log.info(
                                    f"Eject node: {spare_nodes[spare_node_index]}")
                                rest.eject_node(node.id)
                            elif services_or_action[0] \
                                    == "graceful_fo_delta_recovery":
                                self.__perform_fo(
                                    self.cluster.master, node,
                                    reb_topology, nodes_considered,
                                    fo_type="graceful",
                                    recovery_type="delta",
                                    services_in_add_back_node=services_or_action[1])
                            elif services_or_action[0] \
                                    == "graceful_fo_full_recovery":
                                self.__perform_fo(
                                    self.cluster.master, node,
                                    reb_topology, nodes_considered,
                                    fo_type="graceful",
                                    recovery_type="full",
                                    services_in_add_back_node=services_or_action[1])
                            elif services_or_action[0] \
                                    == "graceful_fo_reb_out":
                                self.__perform_fo(
                                    self.cluster.master, node,
                                    reb_topology, nodes_considered,
                                    fo_type="graceful")
                            elif services_or_action[0] \
                                    == "hard_fo_delta_recovery":
                                self.__perform_fo(
                                    self.cluster.master, node,
                                    reb_topology, nodes_considered,
                                    fo_type="hard",
                                    recovery_type="delta",
                                    services_in_add_back_node=services_or_action[1])
                            elif services_or_action[0] \
                                    == "hard_fo_full_recovery":
                                self.__perform_fo(
                                    self.cluster.master, node,
                                    reb_topology, nodes_considered,
                                    fo_type="hard",
                                    recovery_type="full",
                                    services_in_add_back_node=services_or_action[1])
                            elif services_or_action[0] == "hard_fo_reb_out":
                                self.__perform_fo(
                                    self.cluster.master, node,
                                    reb_topology, nodes_considered,
                                    fo_type="hard")
                        else:
                            # No cluster action given, just service remap
                            services = services_or_action
                            for service in services:
                                if service == CbServer.Services.KV:
                                    continue
                                reb_topology[service].append(node.id)
                            nodes_considered.append(node)
        result = self.task.rebalance(self.cluster, to_add=[], to_remove=[],
                                     service_topology=reb_topology)
        self.assertTrue(result, "Rebalance failed")

    @staticmethod
    def run_with_travel_sample_ops(test_function):
        def with_ops(self):
            self.__travel_sample_app_ops("start")
            try:
                test_function(self)
            finally:
                self.__travel_sample_app_ops("stop")
        return with_ops

    @run_with_travel_sample_ops
    def test_add_node_with_topology_update(self):
        impact_kv_nodes = self.input.param("impact_kv_nodes", False)
        req_service_map = [["kv"],
                           ["kv"],
                           ["n1ql", "index"],
                           ["backup", "fts"],
                           ["cbas", "eventing"]]
        self.__set_nodes_with_desired_cluster_config(req_service_map)

        self.log.info("Starting test")
        if impact_kv_nodes:
            remap_service_conf = [
                (["kv"], ["kv", "eventing"]),
                (["kv"], ["kv", "fts"]),
                (["n1ql", "index"], ["backup", "fts"]),
                (["backup", "fts"], ["index", "cbas"]),
                (["cbas", "eventing"], ["n1ql"])]
        else:
            remap_service_conf = [
                (["kv"], ["kv"]),
                (["kv"], ["kv"]),
                (["n1ql", "index"], ["backup", "eventing"]),
                (["backup", "fts"], ["index", "cbas"]),
                (["cbas", "eventing"], ["n1ql", "fts"])]
        for services in self.reb_in_services:
            service_list = services.split(';')
            remap_service_conf.append((service_list, ("reb_in",)))
        self.trigger_remap_service_rebalance(remap_service_conf)

    @run_with_travel_sample_ops
    def test_reb_out_with_topology_update(self):
        impact_kv_nodes = self.input.param("impact_kv_nodes", False)
        req_service_map = [["kv"],
                           ["kv"],
                           ["n1ql"],
                           ["index"],
                           ["backup", "fts"],
                           ["cbas", "eventing"]]
        self.__set_nodes_with_desired_cluster_config(req_service_map)

        self.log.info("Shuffling services post add-nodes")
        if impact_kv_nodes:
            remap_service_conf = [
                (["kv"], ["kv", "n1ql"]),
                (["kv"], ["kv", "index"]),
                (["n1ql", "index"], ["reb_out"]),
                (["backup", "fts"], ["index", "cbas", "eventing"]),
                (["cbas", "eventing"], ["n1ql", "backup", "fts"])]
        else:
            remap_service_conf = [
                (["kv"], ["kv"]),
                (["kv"], ["kv"]),
                (["n1ql"], ["reb_out"]),
                (["index"], ["reb_out"]),
                (["backup", "fts"], ["index", "eventing", "cbas"]),
                (["cbas", "eventing"], ["n1ql", "backup", "fts"])]
        self.trigger_remap_service_rebalance(remap_service_conf)

    @run_with_travel_sample_ops
    def test_reb_in_out_with_topology_update(self):
        impact_kv_nodes = self.input.param("impact_kv_nodes", False)
        req_service_map = [["kv"],
                           ["kv"],
                           ["index", "n1ql", "fts"],
                           ["cbas", "eventing", "backup"]]
        self.__set_nodes_with_desired_cluster_config(req_service_map)

        self.log.info("Shuffling services post add-nodes")
        if impact_kv_nodes:
            remap_service_conf = [
                (["kv"], ["reb_out"]),
                (["index", "n1ql", "fts"], ["reb_out"]),
                (["kv"], ["kv", "fts"]),
                (["backup", "fts"], ["index", "cbas", "eventing"]),
                (["cbas", "eventing"], ["n1ql", "backup", "fts"]),
                (["kv", "index", "n1ql"], ["reb_in"])]
        else:
            remap_service_conf = [
                (["kv"], ["kv"]),
                (["kv"], ["kv"]),
                (["kv"], ["reb_in"]),
                (["index", "n1ql", "fts"], ["reb_out"]),
                (["index"], ["reb_out"]),
                (["backup", "fts"], ["index", "eventing", "cbas"]),
                (["cbas", "eventing"], ["n1ql", "backup", "fts"])]
        self.trigger_remap_service_rebalance(remap_service_conf)

    def test_failover_with_topology_update(self):
        impact_kv_nodes = self.input.param("impact_kv_nodes", False)
        fo_only_kv = self.input.param("fo_only_kv", True)
        multi_node_fo = self.input.param("multi_node_fo", False)
        # This 'fo_type' should be one of the value from,
        # 'trigger_remap_service_rebalance :: valid_action_str' list
        fo_type = self.input.param("fo_type")

        if multi_node_fo:
            # If Multi-node fo, then create replica within services
            req_service_map = [["kv"],
                               ["kv"],
                               ["kv"],
                               ["n1ql", "index", "cbas"],
                               ["fts", "eventing"],
                               ["backup", "fts", "eventing"]]
        else:
            req_service_map = [["kv"],
                               ["kv"],
                               ["kv"],
                               ["n1ql", "index"],
                               ["backup", "fts"],
                               ["cbas", "eventing"]]
        self.__set_nodes_with_desired_cluster_config(req_service_map)

        if multi_node_fo:
            if fo_only_kv:
                # Failover KV + swap services among the other service nodes
                remap_service_conf = [
                    (["kv"], [fo_type]),
                    (["kv"], ["kv"]),
                    (["kv"], ["kv"]),
                    (["n1ql", "index", "cbas"], ["backup", "fts", "eventing"]),
                    (["fts", "eventing"], [fo_type]),
                    (["backup", "fts", "eventing"], ["n1ql", "index", "cbas"])]
            elif impact_kv_nodes:
                # Failover KV + add new services to KV nodes
                remap_service_conf = [
                    (["kv"], [fo_type]),
                    (["kv"], ["kv"]),
                    (["kv"], ["kv"]),
                    (["kv", "index", "eventing", "n1ql", "cbas"], ["reb_in"]),
                    (["n1ql", "index", "cbas"], ["backup", "fts"]),
                    (["fts", "eventing"], [fo_type]),
                    (["backup", "fts", "eventing"], [])]
            else:
                # Failover KV but don't update topology in KV nodes
                remap_service_conf = [
                    (["kv"], [fo_type]),
                    (["kv"], ["kv"]),
                    (["kv"], ["kv"]),
                    (["kv"], ["reb_in"]),
                    (["n1ql", "index", "cbas"], ["backup", "fts", "eventing"]),
                    (["fts", "eventing"], [fo_type]),
                    (["backup", "fts", "eventing"], ["n1ql", "index", "cbas"])]
        else:
            if fo_only_kv:
                # Failover KV + swap services among the other service nodes
                remap_service_conf = [
                    (["kv"], [fo_type]),
                    (["kv"], ["kv"]),
                    (["kv"], ["kv"]),
                    (["n1ql", "index"], ["backup", "fts"]),
                    (["backup", "fts"], ["cbas", "eventing"]),
                    (["cbas", "eventing"], ["n1ql", "index"])]
            elif impact_kv_nodes:
                # Failover KV + add new services to KV nodes
                remap_service_conf = [
                    (["kv"], [fo_type]),
                    (["kv"], ["kv", "index", "n1ql",
                              "eventing", "fts", "cbas"]),
                    (["kv"], ["kv"]),
                    (["kv"], ["reb_in"]),
                    (["n1ql", "index"], []),
                    (["backup", "fts"], ["cbas", "eventing"]),
                    (["cbas", "eventing"], ["backup"])]
            else:
                # Failover KV but don't update topology in KV nodes
                remap_service_conf = [
                    (["kv"], [fo_type]),
                    (["kv"], ["kv"]),
                    (["kv"], ["kv"]),
                    (["kv", "backup", "fts"], ["reb_in"]),
                    (["n1ql", "index"], []),
                    (["backup", "fts"], ["cbas", "eventing"]),
                    (["cbas", "eventing"], ["n1ql", "index"])]

        self.trigger_remap_service_rebalance(remap_service_conf)

    @run_with_travel_sample_ops
    def test_swap_services_between_nodes(self):
        """
        Test swap of services and services-less nodes
        """
        req_service_map = [["kv"],
                           ["kv"],
                           ["n1ql", "index"],
                           ["fts", "backup"],
                           ["cbas"],
                           ["eventing"]]
        self.__set_nodes_with_desired_cluster_config(req_service_map)

        self.log.info("Swap services between nodes")
        remap_service_conf = [
            (["n1ql", "index"], ["cbas"]),
            (["cbas"], ["n1ql", "index"]),
            (["fts", "backup"], ["eventing"]),
            (["eventing"], ["fts", "backup"])]
        self.trigger_remap_service_rebalance(remap_service_conf)

        self.log.info("Test of service-less node swap")
        req_service_map = [["kv"],
                           ["kv"],
                           ["n1ql", "index"],
                           ["fts", "backup"],
                           ["cbas", "eventing"],
                           []]
        self.__set_nodes_with_desired_cluster_config(req_service_map)
        remap_service_conf = [
            (["n1ql", "index"], ["cbas", "eventing"]),
            (["fts", "backup"], []),
            (["cbas", "eventing"], ["n1ql", "index"]),
            ([], ["fts", "backup"])]
        self.trigger_remap_service_rebalance(remap_service_conf)

    def run_app(self):
        default_op_count = 10
        random_op = "random"
        all_tenants = "all"
        # List of supported app activity types
        cluster_activity = "cluster"
        guest_activity = "guest"
        user_activity = "user"
        hotel_activity = "hotel"
        airline_activity = "airline"

        cluster_scenario = Cluster(self.task, self.cluster, self.cluster_util)
        itr_index = 1
        while itr_index <= self.app_iteration:
            self.log.info("#### Iteration :: %d ####" % itr_index)
            tasks = list()
            for activity in self.activities:
                task = None
                tenants = self.tenants
                activity_type = activity.get("type")
                op_type = activity.get("op_type", random_op)
                op_count = activity.get("op_count", default_op_count)
                num_tenant = activity.get("tenants", all_tenants)
                if type(num_tenant) is int:
                    tenants = sample(self.tenants, activity["tenants"])
                if activity_type == user_activity:
                    for tenant in tenants:
                        task = User(self.bucket, tenant,
                                    op_type=op_type,
                                    op_count=op_count)
                elif activity_type == guest_activity:
                    for tenant in tenants:
                        task = Guest(self.bucket, tenant,
                                     op_type=op_type,
                                     op_count=op_count)
                elif activity_type == hotel_activity:
                    for tenant in tenants:
                        task = Hotel(self.bucket, tenant,
                                     op_type=op_type,
                                     op_count=op_count)
                elif activity_type == airline_activity:
                    for tenant in tenants:
                        task = Airline(self.bucket, tenant,
                                       op_type=op_type,
                                       op_count=op_count)
                elif activity_type == cluster_activity:
                    if cluster_scenario.rebalance_task is not None:
                        # Validate running rebalance result
                        if cluster_scenario.rebalance_task:
                            self.task_manager.get_task_result(
                                cluster_scenario.rebalance_task)
                            self.assertTrue(cluster_scenario.rebalance_task,
                                            "Rebalance failure")
                            cluster_scenario.rebalance_task = None

                    services = list()
                    num_nodes = activity.get("nodes", 1)
                    for service in activity.get("service", "kv").split(","):
                        services.append(service.replace(":", ","))

                    len_services = len(services)
                    if len_services != num_nodes:
                        services += [services[-1]] * (num_nodes - len_services)

                    cluster_scenario.run(op_type,
                                         services=services)
                else:
                    self.fail("Unsupported activity_type: %s" % activity_type)

                if task:
                    # Start the activity
                    task.start()
                    # Append the task to the list for tracking
                    tasks.append(task)

            # Wait for threads to complete
            for task in tasks:
                task.join()

            if cluster_scenario.rebalance_task is not None:
                # Validate running rebalance result (if any)
                if cluster_scenario.rebalance_task:
                    self.task_manager.get_task_result(
                        cluster_scenario.rebalance_task)
                    self.assertTrue(cluster_scenario.rebalance_task,
                                    "Rebalance failure")
                    cluster_scenario.rebalance_task = None

            for task in tasks:
                if task.exception:
                    self.fail(task.exception)

            # Print current iteration summary (Possible values)
            # Backup and restore

            # Check for core dumps / critical messages in logs
            result = self.check_coredump_exist(self.servers,
                                               force_collect=True)
            self.assertFalse(result, "CRASH | CRITICAL | WARN messages "
                                     "found in cb_logs")

            if choice(range(0, 9)) == 10:
                query_util.CommonUtil.incr_date(self.tenants)

            itr_index += 1
