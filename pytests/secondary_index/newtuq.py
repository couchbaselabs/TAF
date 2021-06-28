import copy
import json

from gsiLib.gsiHelper import GsiHelper
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from couchbase_helper.tuq_helper import N1QLHelper
from membase.api.rest_client import RestConnection


class QueryTests(BaseTestCase):
    def setUp(self):
        super(QueryTests, self).setUp()
        self.expiry = self.input.param("expiry", 0)
        self.scan_consistency = self.input.param("scan_consistency",
                                                 "request_plus")
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.run_async = self.input.param("run_async", True)
        self.version = self.input.param("cbq_version", "git_repo")
        for server in self.cluster.servers:
            rest = RestConnection(server)
            temp = rest.cluster_status()
            while temp['nodes'][0]['status'] == 'warmup':
                self.log.info("Waiting for cluster to become healthy")
                self.sleep(5)
                temp = rest.cluster_status()

        indexer_node = self.cluster_util.get_nodes_from_services_map(
            service_type="index",
            get_all_nodes=True)
        # Set indexer storage mode
        self.indexer_rest = GsiHelper(indexer_node[0], self.log)
        doc = {"indexer.settings.storage_mode": self.gsi_type}
        self.indexer_rest.set_index_settings_internal(doc)
        doc = {"indexer.api.enableTestServer": True}
        self.indexer_rest.set_index_settings_internal(doc)
        self.indexer_scanTimeout = self.input.param("indexer_scanTimeout",
                                                    None)
        if self.indexer_scanTimeout is not None:
            for server in indexer_node:
                rest = GsiHelper(server, self.log)
                rest.set_index_settings({"indexer.settings.scan_timeout": self.indexer_scanTimeout})
        if self.input.tuq_client and "client" in self.input.tuq_client:
            self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
        else:
            self.shell = RemoteMachineShellConnection(self.cluster.master)
        self.use_gsi_for_primary = self.input.param("use_gsi_for_primary", False)
        self.use_gsi_for_secondary = self.input.param("use_gsi_for_secondary", True)
        self.create_primary_index = self.input.param("create_primary_index", True)
        self.use_rest = self.input.param("use_rest", True)
        self.max_verify = self.input.param("max_verify", None)
        self.buckets = self.bucket_util.get_all_buckets(self.cluster)
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.dataset = self.input.param("dataset", "default")
        self.value_size = self.input.param("value_size", 1024)
        self.doc_ops = self.input.param("doc_ops", False)
        self.create_ops_per = self.input.param("create_ops_per", 0)
        self.expiry_ops_per = self.input.param("expiry_ops_per", 0)
        self.delete_ops_per = self.input.param("delete_ops_per", 0)
        self.update_ops_per = self.input.param("update_ops_per", 0)
        self.nodes_out_dist = self.input.param("nodes_out_dist", None)
        self.targetIndexManager = self.input.param("targetIndexManager", False)
        self.targetMaster = self.input.param("targetMaster", False)
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket()
        self.gens_load = self.generate_docs(self.docs_per_day)
        if self.input.param("gomaxprocs", None):
            self.n1ql_helper.configure_gomaxprocs()
        self.full_docs_list = self.generate_full_docs_list(self.gens_load)
        self.gen_results = TuqGenerators(self.log, self.full_docs_list)
        verify_data = False
        if self.scan_consistency != "request_plus":
            verify_data = True
        self.load(self.gens_load,
                  flag=self.item_flag,
                  verify_data=verify_data)
        if self.doc_ops:
            self.ops_dist_map = self.calculate_data_change_distribution(
                create_per=self.create_ops_per, update_per=self.update_ops_per,
                delete_per=self.delete_ops_per, expiry_per=self.expiry_ops_per,
                start=0, end=self.docs_per_day)
            self.docs_gen_map = self.generate_ops_docs(self.docs_per_day, 0)
            self.full_docs_list_after_ops = \
                self.generate_full_docs_list_after_ops(self.docs_gen_map)
        # Define Helper Method which will be used for running n1ql queries,
        # create index, drop index
        self.n1ql_helper = N1QLHelper(
            version=self.version, shell=self.shell,
            use_rest=self.use_rest, max_verify=self.max_verify,
            buckets=self.cluster.buckets, item_flag=self.item_flag,
            n1ql_port=self.n1ql_port, full_docs_list=self.full_docs_list,
            log=self.log, input=self.input, master=self.cluster.master)
        self.n1ql_node = self.cluster_util.get_nodes_from_services_map(
            service_type="n1ql")
        # self.n1ql_helper._start_command_line_query(self.n1ql_node)
        if self.create_primary_index:
            try:
                self.n1ql_helper.create_primary_index(
                    using_gsi=self.use_gsi_for_primary,
                    server=self.n1ql_node)
            except Exception, ex:
                self.log.info(ex)
                raise ex
        self.log.info("=== QueryTests setUp complete ===")

    def tearDown(self):
        self.check_gsi_logs_for_panic()
        if hasattr(self, 'n1ql_helper'):
            if hasattr(self, 'skip_cleanup') and not self.skip_cleanup:
                self.n1ql_node = self.cluster_util.get_nodes_from_services_map(
                    service_type="n1ql")
                self.n1ql_helper.drop_primary_index(
                    using_gsi=self.use_gsi_for_primary,
                    server=self.n1ql_node)
        if hasattr(self, 'shell') and self.shell is not None:
            if not self.skip_cleanup:
                self.n1ql_helper._restart_indexer()
                self.n1ql_helper.killall_tuq_process()
        super(QueryTests, self).tearDown()

    def generate_docs(self, num_items, start=0):
        try:
            if self.dataset == "simple":
                return self.generate_docs_simple(num_items, start)
            if self.dataset == "sales":
                return self.generate_docs_sales(num_items, start)
            if self.dataset == "bigdata":
                return self.generate_docs_bigdata(num_items, start)
            if self.dataset == "sabre":
                return self.generate_docs_sabre(num_items, start)
            if self.dataset == "array":
                return self.generate_docs_array(num_items, start)
            return getattr(self, 'generate_docs_' + self.dataset)(num_items,
                                                                  start)
        except Exception, ex:
            self.log.error(str(ex))
            self.fail("There is no dataset %s, please enter a valid one"
                      % self.dataset)

    def generate_ops_docs(self, num_items, start=0):
        json_generator = JsonGenerator()
        if self.dataset == "simple":
            return self.generate_ops(num_items, start,
                                     json_generator.generate_docs_simple)
        if self.dataset == "sales":
            return self.generate_ops(num_items, start,
                                     json_generator.generate_docs_sales)
        if self.dataset in ["employee", "default"]:
            return self.generate_ops(num_items, start,
                                     json_generator.generate_docs_employee)
        if self.dataset == "sabre":
            return self.generate_ops(num_items, start,
                                     json_generator.generate_docs_sabre)
        if self.dataset == "bigdata":
            return self.generate_ops(num_items, start,
                                     json_generator.generate_docs_bigdata)
        if self.dataset == "array":
            return self.generate_ops(
                num_items, start,
                json_generator.generate_all_type_documents_for_gsi)
        self.log.error("Invalid dataset: %s" % self.dataset)
        self.fail("Invalid dataset %s, select a valid one" % self.dataset)

    def generate_docs_default(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day, start)

    def generate_docs_sabre(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_sabre(docs_per_day, start)

    def generate_docs_employee(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day=docs_per_day,
                                                     start=start)

    def generate_docs_simple(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_simple(start=start,
                                                   docs_per_day=docs_per_day)

    def generate_docs_sales(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_sales(docs_per_day=docs_per_day,
                                                  start=start)

    def generate_docs_bigdata(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(docs_per_day=docs_per_day,
                                                    start=start,
                                                    value_size=self.value_size)

    def generate_docs_array(self, num_items=10, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_all_type_documents_for_gsi(
            docs_per_day=num_items,
            start=start)

    def generate_ops(self, docs_per_day, start=0, method=None):
        gen_docs_map = {}
        for key in self.ops_dist_map.keys():
            if self.dataset != "bigdata":
                gen_docs_map[key] = method(
                    docs_per_day=self.ops_dist_map[key]["end"],
                    start=self.ops_dist_map[key]["start"])
            else:
                gen_docs_map[key] = method(
                    value_size=self.value_size,
                    end=self.ops_dist_map[key]["end"],
                    start=self.ops_dist_map[key]["start"])
        return gen_docs_map

    def generate_full_docs_list_after_ops(self, gen_docs_map):
        docs = []
        for key in gen_docs_map.keys():
            if key != "delete" and key != "expiry":
                update = False
                if key == "update":
                    update = True
                gen_docs = self.generate_full_docs_list(
                    gens_load=gen_docs_map[key],
                    update=update)
                for doc in gen_docs:
                    docs.append(doc)
        return docs

    def generate_full_docs_list(self, gens_load=None, keys=None, update=False):
        if keys is None:
            keys = []
        if gens_load is None:
            gens_load = []
        all_docs_list = []
        if not isinstance(gens_load, list):
            gens_load = [gens_load]
        for gen_load in gens_load:
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                key, val = doc_gen.next()
                try:
                    val = json.loads(val)
                    if isinstance(val, dict) and 'mutated' not in val.keys():
                        if update:
                            val['mutated'] = 1
                        else:
                            val['mutated'] = 0
                    else:
                        val['mutated'] += val['mutated']
                except TypeError:
                    pass
                if keys:
                    if not (key in keys):
                        continue
                all_docs_list.append(val)
        return all_docs_list

    def async_run_doc_ops(self):
        if self.doc_ops:
            tasks = self.bucket_util.async_ops_all_buckets(
                self.docs_gen_map,
                batch_size=self.batch_size)
            self.n1ql_helper.full_docs_list = self.full_docs_list_after_ops
            self.gen_results = TuqGenerators(self.log,
                                             self.n1ql_helper.full_docs_list)
            self.log.info("------ KV OPS Done ------")
            return tasks
        return []

    def run_doc_ops(self):
        verify_data = True
        if self.scan_consistency == "request_plus":
            verify_data = False
        if self.doc_ops:
            self.bucket_util.sync_ops_all_buckets(
                self.cluster,
                docs_gen_map=self.docs_gen_map,
                batch_size=self.batch_size,
                verify_data=verify_data,
                exp=self.expiry,
                num_items=self.num_items)
            self.n1ql_helper.full_docs_list = self.full_docs_list_after_ops
            self.gen_results = TuqGenerators(self.log,
                                             self.n1ql_helper.full_docs_list)

    def load(self, generators_load, buckets=None, exp=0, flag=0,
             op_type='create', start_items=0,
             verify_data=True):
        if not buckets:
            buckets = self.cluster.buckets
        gens_load = dict()
        for bucket in buckets:
            tmp_gen = list()
            if isinstance(generators_load, list):
                for generator_load in generators_load:
                    tmp_gen.append(copy.deepcopy(generator_load))
            else:
                tmp_gen = copy.deepcopy(generators_load)
            gens_load[bucket.name] = copy.deepcopy(tmp_gen)
        tasks = list()
        items = 0
        for bucket in buckets:
            if isinstance(gens_load[bucket.name], list):
                for gen_load in gens_load[bucket.name]:
                    items += (gen_load.end - gen_load.start)
            else:
                items += gens_load[bucket.name].end \
                         - gens_load[bucket.name].start
        for bucket in buckets:
            self.log.info("%s %s to %s documents..."
                          % (op_type, items, bucket.name))
            if type(gens_load[bucket.name]) is list:
                for gen in gens_load[bucket.name]:
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, gen, op_type, exp,
                        flag, self.persist_to, self.replicate_to,
                        self.batch_size,
                        self.sdk_timeout, self.sdk_compression,
                        print_ops_rate=False,
                        retries=self.sdk_retries))
            else:
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, bucket, gens_load[bucket.name], op_type, exp,
                    flag, self.persist_to, self.replicate_to,
                    self.batch_size,
                    self.sdk_timeout, self.sdk_compression,
                    print_ops_rate=False,
                    retries=self.sdk_retries))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.num_items = items + start_items
        if verify_data:
            self.bucket_util.verify_cluster_stats(self.cluster, self.num_items)

    def check_gsi_logs_for_panic(self):
        """ Checks if a string 'str' is present in goxdcr.log on server
            and returns the number of occurances
        """
        nodes_out_list, index_nodes_out = \
            self.cluster_util.generate_map_nodes_out_dist(
                self.nodes_out_dist,
                self.targetMaster,
                self.targetIndexManager)
        panic_str = "panic"
        indexers = self.cluster_util.get_nodes_from_services_map(
            service_type="index",
            get_all_nodes=True)
        if not indexers:
            return None
        for server in indexers:
            if server not in nodes_out_list:
                shell = RemoteMachineShellConnection(server)
                _, dir = RestConnection(server).diag_eval('filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
                indexer_log = str(dir) + '/indexer.log*'
                count, err = shell.execute_command(
                    "zgrep \"{0}\" {1} | wc -l"
                    .format(panic_str, indexer_log))
                if isinstance(count, list):
                    count = int(count[0])
                else:
                    count = int(count)
                shell.disconnect()
                if count > 0:
                    self.log.info("=== PANIC OBSERVED IN INDEXER LOGS ON SERVER {0} ===".format(server.ip))
        projectors = self.cluster_util.get_nodes_from_services_map(
            service_type="kv",
            get_all_nodes=True)
        if not projectors:
            return None
        for server in projectors:
            if server not in nodes_out_list:
                shell = RemoteMachineShellConnection(server)
                _, dir = RestConnection(server).diag_eval('filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
                projector_log = str(dir) + '/projector.log*'
                count, err = shell.execute_command(
                    "zgrep \"{0}\" {1} | wc -l"
                    .format(panic_str, projector_log))
                if isinstance(count, list):
                    count = int(count[0])
                else:
                    count = int(count)
                shell.disconnect()
                if count > 0:
                    self.log.info("===== PANIC OBSERVED IN PROJECTOR LOGS ON SERVER {0}=====".format(server.ip))

    def calculate_data_change_distribution(self, create_per=0, update_per=0,
                                           delete_per=0, expiry_per=0,
                                           start=0, end=0):
        count = end - start
        change_dist_map = {}
        create_count = int(count * create_per)
        start_pointer = start
        end_pointer = start
        if update_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * update_per)
            change_dist_map["update"] = {"start": start_pointer,
                                         "end": end_pointer}
        if expiry_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * expiry_per)
            change_dist_map["expiry"] = {"start": start_pointer,
                                         "end": end_pointer}
        if delete_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * delete_per)
            change_dist_map["delete"] = {"start": start_pointer,
                                         "end": end_pointer}
        if (1 - (update_per + delete_per + expiry_per)) != 0:
            start_pointer = end_pointer
            end_pointer = end
            change_dist_map["remaining"] = {"start": start_pointer,
                                            "end": end_pointer}
        if create_per != 0:
            change_dist_map["create"] = {"start": end,
                                         "end": create_count + end + 1}
        return change_dist_map

    def find_nodes_in_list(self):
        self.nodes_in_list = self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        self.services_in = self.cluster_util.get_services(self.nodes_in_list,
                                                          self.services_in,
                                                          start_node=0)
