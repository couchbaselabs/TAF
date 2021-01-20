import random
import time

from gsiLib.gsiHelper import GsiHelper
from newtuq import QueryTests
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class BaseSecondaryIndexingTests(QueryTests):

    def setUp(self):
        super(BaseSecondaryIndexingTests, self).setUp()
        self.ansi_join = self.input.param("ansi_join", False)
        self.index_lost_during_move_out = list()
        self.verify_using_index_status = self.input.param("verify_using_index_status", False)
        self.use_replica_when_active_down = self.input.param("use_replica_when_active_down", True)
        self.use_where_clause_in_index = self.input.param("use_where_clause_in_index", False)
        self.scan_consistency = self.input.param("scan_consistency", "request_plus")
        self.scan_vector_per_values = self.input.param("scan_vector_per_values", None)
        self.timeout_for_index_online = self.input.param("timeout_for_index_online", 600)
        self.verify_query_result = self.input.param("verify_query_result", True)
        self.verify_explain_result = self.input.param("verify_explain_result", True)
        self.defer_build = self.input.param("defer_build", True)
        self.build_index_after_create = self.input.param("build_index_after_create", True)
        self.run_query_with_explain = self.input.param("run_query_with_explain", True)
        self.run_query = self.input.param("run_query", True)
        self.graceful = self.input.param("graceful", False)
        self.groups = self.input.param("groups", "all").split(":")
        self.use_rest = self.input.param("use_rest", False)
        self.plasma_dgm = self.input.param("plasma_dgm", False)
        if not self.use_rest:
            query_definition_generator = SQLDefinitionGenerator()
            if self.dataset == "default" or self.dataset == "employee":
                self.query_definitions = query_definition_generator.generate_employee_data_query_definitions()
            if self.dataset == "simple":
                self.query_definitions = query_definition_generator.generate_simple_data_query_definitions()
            if self.dataset == "sabre":
                self.query_definitions = query_definition_generator.generate_sabre_data_query_definitions()
            if self.dataset == "bigdata":
                self.query_definitions = query_definition_generator.generate_big_data_query_definitions()
            if self.dataset == "array":
                self.query_definitions = query_definition_generator.generate_airlines_data_query_definitions()
            self.query_definitions = query_definition_generator.filter_by_group(self.groups, self.query_definitions)
        self.ops_map = self._create_operation_map()
        self.find_nodes_in_list()
        self.nodes_out_list, self.index_nodes_out = \
            self.cluster_util.generate_map_nodes_out_dist(
                self.nodes_out_dist,
                self.targetMaster,
                self.targetIndexManager)
        self.memory_create_list = []
        self.memory_drop_list = []
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.index_loglevel = self.input.param("index_loglevel", None)
        if self.index_loglevel:
            self.set_indexer_logLevel(self.index_loglevel)
        self.gsi_thread = self.task
        self.defer_build = self.defer_build and self.use_gsi_for_secondary
        self.num_index_replicas = self.input.param("num_index_replica", 0)
        self.rest = RestConnection(self.cluster.master)
        self.log.info("=== BaseSecondaryIndexingTests Setup complete ===")

    def tearDown(self):
        super(BaseSecondaryIndexingTests, self).tearDown()

    def create_index(self, bucket, query_definition, deploy_node_info=None,
                     desc=None):
        create_task = self.async_create_index(bucket, query_definition,
                                              deploy_node_info, desc=desc)
        self.task_manager.get_task_result(create_task)
        self.assertTrue(create_task.result, "Create Index failed")
        if self.build_index_after_create:
            if self.defer_build:
                build_index_task = self.async_build_index(
                    bucket,
                    [query_definition.index_name])
                self.task_manager.get_task_result(build_index_task)
                self.assertTrue(build_index_task.result,
                                "Build Index task failed")
            check = self.n1ql_helper.is_index_ready_and_in_list(
                bucket,
                query_definition.index_name,
                server=self.n1ql_node)
            self.assertTrue(check, "Failed to create index %s"
                                   % query_definition.index_name)

    def async_create_index(self, bucket, query_definition,
                           deploy_node_info=None, desc=None):
        index_where_clause = None
        if self.use_where_clause_in_index:
            index_where_clause = query_definition.index_where_clause
        self.query = query_definition.generate_index_create_query(
            bucket=bucket,
            use_gsi_for_secondary=self.use_gsi_for_secondary,
            deploy_node_info=deploy_node_info,
            defer_build=self.defer_build,
            index_where_clause=index_where_clause,
            num_replica=self.num_index_replicas, desc=desc)
        create_index_task = self.gsi_thread.async_create_index(
            server=self.n1ql_node,
            bucket=bucket,
            query=self.query,
            n1ql_helper=self.n1ql_helper,
            index_name=query_definition.index_name,
            defer_build=self.defer_build)
        return create_index_task

    def create_index_using_rest(self, bucket, query_definition,
                                exprType='N1QL', deploy_node_info=None,
                                desc=None):
        ind_content = \
            query_definition.generate_gsi_index_create_query_using_rest(
                bucket=bucket,
                deploy_node_info=deploy_node_info,
                defer_build=None,
                index_where_clause=None,
                gsi_type=self.gsi_type,
                desc=desc)

        self.log.info("Creating index %s" % query_definition.index_name)
        return self.indexer_rest.create_index(ind_content)

    def async_build_index(self, bucket, index_list=None):
        if not index_list:
            index_list = list()
        self.query = self.n1ql_helper.gen_build_index_query(
            bucket=bucket,
            index_list=index_list)
        self.log.info(self.query)
        build_index_task = self.gsi_thread.async_build_index(
            server=self.n1ql_node,
            bucket=bucket,
            query=self.query,
            n1ql_helper=self.n1ql_helper)
        return build_index_task

    def async_monitor_index(self, bucket, index_name=None):
        monitor_index_task = self.gsi_thread.async_monitor_index(
            server=self.n1ql_node,
            bucket=bucket,
            n1ql_helper=self.n1ql_helper,
            index_name=index_name,
            timeout=self.timeout_for_index_online)
        return monitor_index_task

    def multi_create_index(self, buckets=None, query_definitions=None,
                           deploy_node_info=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = "%s:%s" % (bucket.name,
                                        query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.create_index(bucket.name,
                                      query_definition,
                                      deploy_node_info)

    def multi_create_index_using_rest(self, buckets=None,
                                      query_definitions=None,
                                      deploy_node_info=None):
        self.index_id_map = {}
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            if bucket not in self.index_id_map.keys():
                self.index_id_map[bucket] = {}
            for query_definition in query_definitions:
                id_map = self.create_index_using_rest(
                    bucket=bucket,
                    query_definition=query_definition,
                    deploy_node_info=deploy_node_info)
                self.index_id_map[bucket][query_definition] = id_map["id"]

    def async_multi_create_index(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        create_index_tasks = []
        self.index_lost_during_move_out = []
        self.log.info(self.index_nodes_out)
        index_node_count = 0
        for query_definition in query_definitions:
                index_info = "{0}".format(query_definition.index_name)
                if index_info not in self.memory_create_list:
                    self.memory_create_list.append(index_info)
                    self.deploy_node_info = None
                    if index_node_count < len(self.index_nodes_out):
                        node_index = index_node_count
                        self.deploy_node_info = ["{0}:{1}".format(self.index_nodes_out[index_node_count].ip,
                        self.index_nodes_out[index_node_count].port)]
                        if query_definition.index_name not in self.index_lost_during_move_out:
                            self.index_lost_during_move_out.append(query_definition.index_name)
                        index_node_count += 1
                    for bucket in buckets:
                        create_index_tasks.append(self.async_create_index(bucket.name,
                            query_definition, deploy_node_info = self.deploy_node_info))
                    self.sleep(3)
        if self.defer_build and self.build_index_after_create:
            index_list = []
            for task in create_index_tasks:
                task.result()
            for query_definition in query_definitions:
                if query_definition.index_name not in index_list:
                    index_list.append(query_definition.index_name)
            for bucket in self.bucket_util.buckets:
                build_index_task = self.async_build_index(bucket, index_list)
                build_index_task.result()
            monitor_index_tasks = []
            for index_name in index_list:
                for bucket in self.bucket_util.buckets:
                    monitor_index_tasks.append(self.async_monitor_index(bucket.name, index_name))
            return monitor_index_tasks
        else:
            return create_index_tasks

    def multi_drop_index_using_rest(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.drop_index_using_rest(bucket, query_definition)

    def multi_drop_index(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(bucket = bucket.name)
                index_create_info = "{0}:{1}".format(bucket.name, query_definition.index_name)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    self.drop_index(bucket.name, query_definition)
                if index_create_info in self.memory_create_list:
                    self.memory_create_list.remove(index_create_info)

    def async_multi_drop_index(self, buckets=None, query_definitions=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        drop_index_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                index_info = query_definition.generate_index_drop_query(bucket = bucket.name)
                if index_info not in self.memory_drop_list:
                    self.memory_drop_list.append(index_info)
                    drop_index_tasks.append(self.async_drop_index(bucket.name, query_definition))
        return drop_index_tasks

    def drop_index(self, bucket, query_definition, verify_drop=True):
        try:
            self.query = query_definition.generate_index_drop_query(bucket = bucket,
            use_gsi_for_secondary = self.use_gsi_for_secondary, use_gsi_for_primary = self.use_gsi_for_primary)
            actual_result = self.n1ql_helper.run_cbq_query(query = self.query, server = self.n1ql_node)
            if verify_drop:
                check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name, server = self.n1ql_node)
                self.assertFalse(check, "index {0} failed to be deleted".format(query_definition.index_name))
        except Exception, ex:
            self.log.info(ex)
            query = "select * from system:indexes"
            actual_result = self.n1ql_helper.run_cbq_query(
                query=query,
                server=self.n1ql_node)
            self.log.info(actual_result)

    def drop_index_using_rest(self, bucket, query_definition, verify_drop=True):
        self.log.info("Dropping index: %s" % query_definition.index_name)
        self.rest.drop_index_with_rest(self.index_id_map[bucket][query_definition])
        if verify_drop:
            check = self.n1ql_helper._is_index_in_list(bucket, query_definition.index_name, server=self.n1ql_node)
            self.assertFalse(check, "Failed to delete Index %s"
                             % query_definition.index_name)
            del(self.index_id_map[bucket][query_definition])

    def async_drop_index(self, bucket, query_definition):
        self.query = query_definition.generate_index_drop_query(
            bucket=bucket,
            use_gsi_for_secondary=self.use_gsi_for_secondary,
            use_gsi_for_primary=self.use_gsi_for_primary)
        drop_index_task = self.gsi_thread.async_drop_index(
            server=self.n1ql_node,
            bucket=bucket,
            query=self.query,
            n1ql_helper=self.n1ql_helper,
            index_name=query_definition.index_name)
        return drop_index_task

    def query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket=bucket)
        actual_result = self.n1ql_helper.run_cbq_query(query=self.query, server=self.n1ql_node)
        self.log.info(actual_result)
        if self.verify_explain_result:
            check = self.n1ql_helper.verify_index_with_explain(actual_result, query_definition.index_name)
            self.assertTrue(check, "Index %s not found"
                                   % query_definition.index_name)

    def async_query_using_index_with_explain(self, bucket, query_definition):
        self.query = query_definition.generate_query_with_explain(bucket=bucket)
        query_with_index_task = self.gsi_thread.async_n1ql_query_verification(
            server=self.n1ql_node,
            bucket=bucket,
            query=self.query,
            n1ql_helper=self.n1ql_helper,
            is_explain_query=True,
            index_name=query_definition.index_name,
            verify_results=self.verify_explain_result)
        return query_with_index_task

    def multi_query_using_index_with_explain(self, buckets=None,
                                             query_definitions=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_explain(bucket.name,
                                                    query_definition)

    def async_multi_query_using_index_with_explain(self, buckets=None,
                                                   query_definitions=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        async_query_with_explain_tasks = []
        for bucket in buckets:
            for query_definition in query_definitions:
                async_query_with_explain_tasks.append(
                    self.async_query_using_index_with_explain(
                        bucket.name,
                        query_definition))
        return async_query_with_explain_tasks

    def query_using_index(self, bucket, query_definition, expected_result=None,
                          scan_consistency=None,
                          scan_vector=None, verify_results=True):
        if not scan_consistency:
            scan_consistency = self.scan_consistency
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        if expected_result is None:
            expected_result = self.gen_results.generate_expected_result(
                print_expected_result=False)
        self.query = self.gen_results.query
        self.log.info("Query : {0}".format(self.query))
        msg, check = self.n1ql_helper.run_query_and_verify_result(
            query=self.query,
            server=self.n1ql_node,
            timeout=500,
            expected_result=expected_result,
            scan_consistency=scan_consistency,
            scan_vector=scan_vector,
            verify_results=verify_results)
        self.assertTrue(check, msg)

    def async_query_using_index(self, bucket, query_definition,
                                expected_result=None,
                                scan_consistency=None,
                                scan_vector=None):
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        if expected_result is None:
            expected_result = self.gen_results.generate_expected_result(
                print_expected_result = False)
        self.query = self.gen_results.query
        query_with_index_task = self.gsi_thread.async_n1ql_query_verification(
            server=self.n1ql_node,
            bucket=bucket,
            query=self.query,
            n1ql_helper=self.n1ql_helper,
            expected_result=expected_result,
            index_name=query_definition.index_name,
            scan_consistency=scan_consistency,
            scan_vector=scan_vector,
            verify_results=self.verify_query_result)
        return query_with_index_task

    def query_using_index_with_emptyset(self, bucket, query_definition):
        self.gen_results.query = query_definition.generate_query(bucket=bucket)
        self.log.info("Query : {0}".format(self.gen_results.query))
        self.query = self.gen_results.query
        actual_result = self.n1ql_helper.run_cbq_query(query=self.query,
                                                       server=self.n1ql_node)
        self.assertTrue(len(actual_result["results"]) == 0,
                        "Result is not empty %s" % actual_result["results"])

    def multi_query_using_index_with_emptyresult(self, buckets=None,
                                                 query_definitions=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            for query_definition in query_definitions:
                self.query_using_index_with_emptyset(bucket.name,
                                                     query_definition)

    def multi_query_using_index(self, buckets=None, query_definitions=None,
                                expected_results=None, scan_consistency=None,
                                scan_vectors=None, verify_results=True):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        for bucket in buckets:
            scan_vector = None
            if scan_vectors is not None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    expected_result = expected_results[query_definition.index_name]
                else:
                    expected_result = None
                self.query_using_index(bucket=bucket.name, query_definition=query_definition,
                                       expected_result=expected_result, scan_consistency=scan_consistency,
                                       scan_vector=scan_vector, verify_results=verify_results)

    def async_multi_query_using_index(self, buckets=[], query_definitions=[],
                                      expected_results={},
                                      scan_consistency=None,
                                      scan_vectors=None):
        multi_query_tasks = []
        for bucket in buckets:
            scan_vector = None
            if scan_vectors is not None:
                scan_vector = scan_vectors[bucket.name]
            for query_definition in query_definitions:
                if expected_results:
                    multi_query_tasks.append(self.async_query_using_index(
                        bucket.name, query_definition,
                        expected_results[query_definition.index_name],
                        scan_consistency=scan_consistency,
                        scan_vector=scan_vector))
                else:
                    multi_query_tasks.append(self.async_query_using_index(
                        bucket.name, query_definition, None,
                        scan_consistency=scan_consistency,
                        scan_vector=scan_vector))
        return multi_query_tasks

    def async_check_and_run_operations(self, buckets=[], initial=False,
                                       before=False, after=False,
                                       in_between=False, scan_consistency=None,
                                       scan_vectors=None):
        if initial:
            self._set_query_explain_flags("initial")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(
                buckets=buckets,
                create_index=self.ops_map["initial"]["create_index"],
                drop_index=self.ops_map["initial"]["drop_index"],
                query=self.ops_map["initial"]["query_ops"],
                query_with_explain=self.ops_map["initial"]["query_explain_ops"],
                scan_consistency=scan_consistency,
                scan_vectors=scan_vectors)
        if before:
            self._set_query_explain_flags("before")
            self.log.info(self.ops_map["before"])
            return self.async_run_multi_operations(
                buckets=buckets,
                create_index=self.ops_map["before"]["create_index"] ,
                drop_index=self.ops_map["before"]["drop_index"],
                query=self.ops_map["before"]["query_ops"],
                query_with_explain=self.ops_map["before"]["query_explain_ops"],
                scan_consistency=scan_consistency,
                scan_vectors=scan_vectors)
        if in_between:
            self._set_query_explain_flags("in_between")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(
                buckets=buckets,
                create_index=self.ops_map["in_between"]["create_index"],
                drop_index=self.ops_map["in_between"]["drop_index"],
                query=self.ops_map["in_between"]["query_ops"],
                query_with_explain=self.ops_map["in_between"]["query_explain_ops"],
                scan_consistency=scan_consistency,
                scan_vectors=scan_vectors)
        if after:
            self._set_query_explain_flags("after")
            self.log.info(self.ops_map["initial"])
            return self.async_run_multi_operations(
                buckets=buckets,
                create_index=self.ops_map["after"]["create_index"],
                drop_index=self.ops_map["after"]["drop_index"],
                query=self.ops_map["after"]["query_ops"],
                query_with_explain=self.ops_map["after"]["query_explain_ops"],
                scan_consistency="request_plus",
                scan_vectors=scan_vectors)

    def run_multi_operations(self, buckets=[], query_definitions=[],
                             expected_results={},
                             create_index=False, drop_index=False,
                             query_with_explain=False, query=False,
                             scan_consistency=None, scan_vectors=None):
        try:
            if create_index:
                self.multi_create_index(buckets, query_definitions)
            if query_with_explain:
                self.multi_query_using_index_with_explain(buckets,
                                                          query_definitions)
            if query:
                self.multi_query_using_index(
                    buckets,
                    query_definitions,
                    expected_results,
                    scan_consistency=scan_consistency,
                    scan_vectors=scan_vectors)
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            if drop_index and not self.skip_cleanup:
                self.multi_drop_index(buckets,query_definitions)

    def async_run_multi_operations(self, buckets=None, query_definitions=None,
                                   expected_results=None,
                                   create_index=False, drop_index=False,
                                   query_with_explain=False, query=False,
                                   scan_consistency=None, scan_vectors=None):
        tasks = []
        if not query_definitions:
            query_definitions = self.query_definitions
        try:
            if create_index:
                tasks += self.async_multi_create_index(buckets,
                                                       query_definitions)
            if query_with_explain:
                tasks += self.async_multi_query_using_index_with_explain(
                    buckets, query_definitions)
            if query:
                tasks += self.async_multi_query_using_index(
                    buckets, query_definitions, expected_results,
                    scan_consistency=scan_consistency,
                    scan_vectors=scan_vectors)
            if drop_index:
                tasks += self.async_multi_drop_index(self.bucket_util.buckets,
                                                     query_definitions)
        except Exception, ex:
            self.log.info(ex)
            raise
        return tasks

    def async_run_operations(self, phase, buckets=None, query_definitions=None,
                             expected_results=None,
                             scan_consistency=None, scan_vectors=None):
        if not buckets:
            buckets = self.bucket_util.buckets
        if not query_definitions:
            query_definitions = self.query_definitions
        if not scan_consistency:
            scan_consistency = self.scan_consistency
        tasks = []
        operation_map = self.generate_operation_map(phase)
        self.log.info("=== {0}: {1} ===".format(phase.upper(), operation_map))
        nodes_out = []
        if isinstance(self.nodes_out_dist, str):
            for service in self.nodes_out_dist.split("-"):
                nodes_out.append(service.split(":")[0])
        if operation_map:
            try:
                if "create_index" in operation_map:
                    if ("index" in nodes_out or "n1ql" in nodes_out) and phase == "in_between":
                            tasks = []
                    else:
                        tasks += self.async_multi_create_index(buckets,
                                                               query_definitions)
                if "query_with_explain" in operation_map:
                    if "n1ql" in nodes_out and phase == "in_between":
                        tasks = []
                    else:
                        tasks += self.async_multi_query_using_index_with_explain(buckets, query_definitions)
                if "query" in operation_map:
                    if "n1ql" in nodes_out and phase == "in_between":
                        tasks = []
                    else:
                        tasks += self.async_multi_query_using_index(
                            buckets, query_definitions, expected_results,
                            scan_consistency=scan_consistency,
                            scan_vectors=scan_vectors)
                if "drop_index" in operation_map:
                    if "index" in nodes_out or "n1ql" in nodes_out:
                        if phase == "in_between":
                            tasks = []
                    else:
                        tasks += self.async_multi_drop_index(
                            self.bucket_util.buckets, query_definitions)
            except Exception, ex:
                self.log.error(ex)
                raise
        return tasks

    def run_full_table_scan_using_rest(self, bucket, query_definition,
                                       verify_result=False):
        expected_result = []
        actual_result = []
        full_scan_query = "SELECT * FROM %s WHERE %s" \
                          % (bucket.name, query_definition.index_where_clause)
        self.gen_results.query = full_scan_query
        temp = self.gen_results.generate_expected_result(
            print_expected_result=False)
        for item in temp:
            expected_result.append(item.values())
        if self.scan_consistency == "request_plus":
            body = {"stale": "False"}
        else:
            body = {"stale": "ok"}
        content = self.rest.full_table_scan_gsi_index_with_rest(self.index_id_map[bucket][query_definition], body)
        if verify_result:
            doc_id_list = []
            for item in content:
                if item["docid"] not in doc_id_list:
                    for doc in self.full_docs_list:
                        if doc["_id"] == item["docid"]:
                            actual_result.append([doc])
                            doc_id_list.append(item["docid"])
            self.assertEqual(len(sorted(actual_result)), len(sorted(expected_result)),
                             "Actual Items {0} are not equal to expected Items {1}".
                             format(len(sorted(actual_result)), len(sorted(expected_result))))
            msg = "The number of rows match but the results mismatch, please check"
            if sorted(actual_result) != sorted(expected_result):
                raise Exception(msg)

    def run_lookup_gsi_index_with_rest(self, bucket, query_definition):
        pass

    def run_range_scan_with_rest(self, bucket, query_definition):
        pass

    def gen_scan_vector(self, use_percentage=1.0, use_random=False):
        servers = self.cluster_util.get_kv_nodes(servers=self.cluster.servers[:self.nodes_init])
        sequence_bucket_map = self.bucket_util.get_vbucket_seqnos(
            servers,
            self.bucket_util.buckets)
        scan_vectors = dict()
        if use_percentage == 1.0:
            for bucket in self.bucket_util.buckets:
                scan_vector = []
                self.log.debug("analyzing for bucket {0}".format(bucket.name))
                map = sequence_bucket_map[bucket.name]
                for i in range(1024):
                    key = "vb_" + str(i)
                    value = [int(map[key]["abs_high_seqno"]), map[key]["uuid"]]
                    scan_vector.append(value)
                scan_vectors[bucket.name] = scan_vector
        else:
            for bucket in self.bucket_util.buckets:
                scan_vector = {}
                total = int(self.cluster_util.vbuckets*use_percentage)
                vbuckets_number_list = range(0,total)
                if use_random:
                    vbuckets_number_list = random.sample(
                        xrange(0, self.cluster_util.vbuckets),
                        total)
                self.log.debug("analyzing for bucket {0}".format(bucket.name))
                map = sequence_bucket_map[bucket.name]
                for key in map.keys():
                    vb = int(key.split("vb_")[1])
                    if vb in vbuckets_number_list:
                        value = [int(map[key]["abs_high_seqno"]), map[key]["uuid"]]
                        scan_vector[str(vb)] = value
                scan_vectors[bucket.name] = scan_vector
        return scan_vectors

    def check_missing_and_extra(self, actual, expected):
        missing = []
        extra = []
        for item in actual:
            if not (item in expected):
                extra.append(item)
        for item in expected:
            if not (item in actual):
                missing.append(item)
        return missing, extra

    def _verify_results(self, actual_result, expected_result, missing_count=1,
                        extra_count=1):
        actual_result = self._gen_dict(actual_result)
        expected_result = self._gen_dict(expected_result)
        if len(actual_result) != len(expected_result):
            missing, extra = self.check_missing_and_extra(actual_result,
                                                          expected_result)
            self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:missing_count], extra[:extra_count]))
            self.fail("Results are incorrect.Actual num %s. Expected num: %s.\n"
                      % (len(actual_result), len(expected_result)))
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]

        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s" +\
        "Expected first and last 100: %s.\n  ... \n %s"
        self.assertTrue(actual_result == expected_result,
                          msg % (actual_result[:100], actual_result[-100:],
                                 expected_result[:100], expected_result[-100:]))

    def verify_index_absence(self, query_definitions, buckets):
        server = self.cluster_util.get_nodes_from_services_map(service_type="n1ql")
        for bucket in buckets:
            for query_definition in query_definitions:
                check = self.n1ql_helper._is_index_in_list(
                    bucket.name,
                    query_definition.index_name,
                    server=server)
                self.assertFalse(check, " {0} was not absent as expected"
                                        .format(query_definition.index_name))

    def _gen_dict(self, result):
        result_set = []
        if result is not None and len(result) > 0:
            for val in result:
                for key in val.keys():
                    result_set.append(val[key])
        return result_set

    def _verify_index_map(self):
        if not self.verify_using_index_status:
            return
        index_map = self.indexer_rest.index_status()
        index_bucket_map = self.n1ql_helper.gen_index_map(self.n1ql_node)
        msg = "difference in index map found, expected {0} \n actual {1}".format(index_bucket_map,index_map)
        self.assertTrue(len(index_map.keys()) == len(self.bucket_util.buckets),
            "numer of buckets mismatch :: "+msg)
        for bucket in self.bucket_util.buckets:
            self.assertTrue((bucket.name in index_map.keys()), " bucket name not present in index map {0}".format(index_map))
        for bucket_name in index_bucket_map.keys():
            self.assertTrue(len(index_bucket_map[bucket_name].keys()) == len(index_map[bucket_name].keys()),"number of indexes mismatch ::"+msg)
            for index_name in index_bucket_map[bucket_name].keys():
                msg1 ="index_name {0} not found in {1}".format(index_name, index_map[bucket_name].keys())
                self.assertTrue(index_name in index_map[bucket_name].keys(), msg1+" :: "+ msg)

    def _verify_primary_index_count(self):
        bucket_map = self.bucket_util.get_buckets_itemCount()
        count = 0
        while not self._verify_items_count() and count < 15:
            self.log.debug("All Items Yet to be Indexed...")
            self.sleep(5)
            count += 1
        self.assertTrue(self._verify_items_count(), "All Items didn't get Indexed...")
        self.log.info("All the documents are indexed...")
        self.sleep(10)
        index_bucket_map = self.n1ql_helper.get_index_count_using_primary_index(self.bucket_util.buckets, self.n1ql_node)
        self.log.info(bucket_map)
        self.log.info(index_bucket_map)
        for bucket_name in bucket_map.keys():
            actual_item_count = index_bucket_map[bucket_name]
            expected_item_count = bucket_map[bucket_name]
            self.assertTrue(str(actual_item_count) == str(expected_item_count),
                "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                    (bucket_name, "primary", expected_item_count, actual_item_count))

    def _verify_items_count(self):
        """
        Compares Items indexed count is sample
        as items in the bucket.
        """
        index_map = self.cluster_util.get_index_stats()
        for bucket_name in index_map.keys():
            self.log.info("Bucket: {0}".format(bucket_name))
            for index_name, index_val in index_map[bucket_name].iteritems():
                self.log.info("Index: {0}".format(index_name))
                self.log.info("number of docs pending: {0}".format(index_val["num_docs_pending"]))
                self.log.info("number of docs queued: {0}".format(index_val["num_docs_queued"]))
                if index_val["num_docs_pending"] and index_val["num_docs_queued"]:
                    return False
        return True

    def _verify_bucket_count_with_index_count(self, query_definitions=None,
                                              buckets=None):
        """
        :param query_definitions: Query definition
        :param buckets: List of bucket objects to verify
        :return:
        """
        count = 0
        if not query_definitions:
            query_definitions = self.query_definitions
        if not buckets:
            buckets = self.bucket_util.buckets
        while not self._verify_items_count() and count < 15:
            self.log.info("All Items Yet to be Indexed...")
            self.sleep(10)
            count += 1
        if not self._verify_items_count():
            raise Exception("All Items didn't get Indexed...")
        bucket_map = self.bucket_util.get_buckets_itemCount()
        for bucket in buckets:
            bucket_count = bucket_map[bucket.name]
            for query in query_definitions:
                index_count = self.n1ql_helper.get_index_count_using_index(bucket,
                                                                           query.index_name, self.n1ql_node)
                self.assertTrue(int(index_count) == int(bucket_count),
                        "Bucket {0}, mismatch in item count for index :{1} : expected {2} != actual {3} ".format
                        (bucket.name, query.index_name, bucket_count, index_count))
        self.log.info("Items Indexed Verified with bucket count...")

    def _check_all_bucket_items_indexed(self, query_definitions=None, buckets=None):
        """
        :param bucket:
        :param index:
        :return:
        """
        count = 0
        while not self._verify_items_count() and count < 15:
            self.sleep(10, "Wait for all items to get indexed")
            count += 1
        self.assertTrue(self._verify_items_count(),
                        "All items didn't get indexed")

    def _create_operation_map(self):
        map_initial = {"create_index":False, "query_ops": False,
                       "query_explain_ops": False, "drop_index": False}
        map_before = {"create_index":False, "query_ops": False,
                      "query_explain_ops": False, "drop_index": False}
        map_in_between = {"create_index":False, "query_ops": False,
                          "query_explain_ops": False, "drop_index": False}
        map_after = {"create_index":False, "query_ops": False,
                     "query_explain_ops": False, "drop_index": False}
        initial = self.input.param("initial", "")
        for op_type in initial.split(":"):
            if op_type != '':
                map_initial[op_type] = True
        before = self.input.param("before", "")
        for op_type in before.split(":"):
            if op_type != '':
                map_before[op_type] = True
        in_between = self.input.param("in_between", "")
        for op_type in in_between.split(":"):
            if op_type != '':
                map_in_between[op_type] = True
        after = self.input.param("after", "")
        for op_type in after.split(":"):
            if op_type != '':
                map_after[op_type] = True
        return {"initial":map_initial, "before":map_before,
                "in_between": map_in_between, "after": map_after}

    def generate_operation_map(self, phase):
        operation_map = []
        self.verify_query_result = False
        self.verify_explain_result = False
        ops = self.input.param(phase, "")
        for type in ops.split("-"):
            for op_type in type.split(":"):
                if "verify_query_result" in op_type:
                    self.verify_query_result = True
                    continue
                if "verify_explain_result" in op_type:
                    self.verify_explain_result = True
                    continue
                if op_type != '':
                    operation_map.append(op_type)
        return operation_map

    def _query_explain_in_async(self):
        tasks = self.async_run_multi_operations(
            buckets=self.bucket_util.buckets,
            query_definitions=self.query_definitions,
            create_index=False, drop_index=False,
            query_with_explain=self.run_query_with_explain,
            query=False, scan_consistency=self.scan_consistency)
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def _set_query_explain_flags(self, phase):
        if ("query_ops" in self.ops_map[phase].keys()) and self.ops_map[phase]["query_ops"]:
            self.ops_map[phase]["query_explain_ops"] = True
        if ("do_not_verify_query_result" in self.ops_map[phase].keys()) \
                and self.ops_map[phase]["do_not_verify_query_result"]:
            self.verify_query_result = False
            self.ops_map[phase]["query_explain_ops"] = False
        if ("do_not_verify_explain_result" in self.ops_map[phase].keys()) \
                and self.ops_map[phase]["do_not_verify_explain_result"]:
            self.verify_explain_result = False
            self.ops_map[phase]["query_explain_ops"] = False
        self.log.info(self.ops_map)

    def set_indexer_logLevel(self, loglevel="info"):
        """
        :param loglevel:
        Possible Values
            -- info
            -- debug
            -- warn
            -- verbose
            -- Silent
            -- Fatal
            -- Error
            -- Timing
            -- Trace
        """
        self.log.info("Setting indexer log level to {0}".format(loglevel))
        server = self.cluster_util.get_nodes_from_services_map(
            service_type="index")
        rest = RestConnection(server)
        status = rest.set_indexer_params("logLevel", loglevel)

    def wait_until_cluster_is_healthy(self):
        master_node = self.cluster.master
        if self.targetMaster:
            if len(self.cluster.servers) > 1:
                master_node = self.cluster.servers[1]
        rest = RestConnection(master_node)
        is_cluster_healthy = False
        count = 0
        while not is_cluster_healthy and count < 10:
            count += 1
            cluster_nodes = rest.node_statuses()
            for node in cluster_nodes:
                if node.status != "healthy":
                    is_cluster_healthy = False
                    self.log.info("Node: {0}, state: {1}"
                                  .format(node.ip, node.status))
                    self.sleep(5)
                    break
                else:
                    is_cluster_healthy = True
        return is_cluster_healthy

    def wait_until_indexes_online(self, timeout=600):
        rest = RestConnection(self.cluster.master)
        init_time = time.time()
        check = False
        next_time = init_time
        while not check:
            index_status = self.indexer_rest.index_status()
            for index_info in index_status.values():
                for index_state in index_info.values():
                    if index_state["status"] == "Ready":
                        check = True
                    else:
                        check = False
                        self.sleep(1, "Will retry getting index status")
                        next_time = time.time()
                        break
            check = check or (next_time - init_time > timeout)
        return check

    def get_dgm_for_plasma(self, indexer_nodes=None, memory_quota=256):
        """
        Internal Method to create OOM scenario
        :return:
        """
        def validate_disk_writes(index_nodes=None):
            if not index_nodes:
                index_nodes = self.cluster_util.get_nodes_from_services_map(
                    service_type="index", get_all_nodes=True)
            for node in index_nodes:
                indexer_rest = GsiHelper(node, self.log)
                content = indexer_rest.get_index_storage_stats()
                for index in content.values():
                    for stats in index.values():
                        if stats["MainStore"]["resident_ratio"] >= 1.00:
                            return False
            return True

        def kv_mutations(num_docs=1):
            if not num_docs:
                num_docs = self.docs_per_day
            gens_load = self.generate_docs(num_docs)
            self.full_docs_list = self.generate_full_docs_list(gens_load)
            self.gen_results = TuqGenerators(self.log, self.full_docs_list)
            self.load(gens_load,
                      buckets=self.bucket_util.buckets,
                      flag=self.item_flag,
                      verify_data=False)
        if self.gsi_type != "plasma":
            return
        if not self.plasma_dgm:
            return
        self.log.info("Setting indexer memory quota to %s MB" % memory_quota)
        node = self.cluster_util.get_nodes_from_services_map(
            service_type="index")
        rest = RestConnection(node)
        rest.set_service_memoryQuota(service='indexMemoryQuota',
                                     memoryQuota=memory_quota)
        cnt = 0
        docs = 50 + self.docs_per_day
        while cnt < 100:
            if validate_disk_writes(indexer_nodes):
                self.log.info("========== DGM is achieved ==========")
                return True
            kv_mutations(docs)
            self.sleep(30)
            cnt += 1
            docs += 20
        return False

    def reboot_node(self, node):
        self.log.info("Rebooting node '{0}'....".format(node.ip))
        shell = RemoteMachineShellConnection(node)
        if shell.extract_remote_info().type.lower() == 'windows':
            o, r = shell.execute_command("shutdown -r -f -t 0")
        elif shell.extract_remote_info().type.lower() == 'linux':
            o, r = shell.execute_command("reboot")
        shell.log_command_output(o, r)
        shell.disconnect()
        # wait for restart and warmup on all node
        self.sleep(300)
        # disable firewall on these nodes
        self.cluster_util.stop_firewall_on_node(node)
        # wait till node is ready after warmup
        self.cluster_util.wait_for_ns_servers_or_assert([node], self)
