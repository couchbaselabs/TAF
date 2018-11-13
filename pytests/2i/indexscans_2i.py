from base_2i import BaseSecondaryIndexingTests
import copy
from couchbase_helper.query_definitions import QueryDefinition
from couchbase_helper.query_definitions import SQLDefinitionGenerator
from couchbase_helper.tuq_generators import TuqGenerators
QUERY_TEMPLATE = "SELECT {0} FROM %s "

class SecondaryIndexingScanTests(BaseSecondaryIndexingTests):

    def setUp(self):
        super(SecondaryIndexingScanTests, self).setUp()

    def tearDown(self):
        super(SecondaryIndexingScanTests, self).tearDown()

    def _create_index_in_async(self, query_definitions = None, buckets = None, index_nodes = None):
        refer_index = []
        if buckets == None:
            buckets = self.buckets
        if query_definitions == None:
            query_definitions = self.query_definitions
        if not self.run_async:
            self.run_multi_operations(buckets=buckets, query_definitions=query_definitions, create_index=True)
            return
        if index_nodes == None:
            index_nodes = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        x =  len(query_definitions) - 1
        while x > -1:
            tasks = []
            build_index_map = {}
            for bucket in buckets:
                build_index_map[bucket.name] = []
            for server in index_nodes:
                for bucket in buckets:
                    if (x > -1):
                        key = "{0}:{1}".format(bucket.name, query_definitions[x].index_name)
                        if (key not in refer_index):
                            refer_index.append(key)
                            refer_index.append(query_definitions[x].index_name)
                            deploy_node_info = None
                            if self.use_gsi_for_secondary:
                                deploy_node_info = ["{0}:{1}".format(server.ip,server.port)]
                            build_index_map[bucket.name].append(query_definitions[x].index_name)
                            tasks.append(self.async_create_index(bucket.name, query_definitions[x],
                                                                 deploy_node_info=deploy_node_info))
                x -= 1
            for task in tasks:
                task.result()
            if self.defer_build:
                for bucket_name in build_index_map.keys():
                    if len(build_index_map[bucket_name]) > 0:
                        build_index_task = self.async_build_index(bucket_name, build_index_map[bucket_name])
                        build_index_task.result()
                monitor_index_tasks = []
                for bucket_name in build_index_map.keys():
                    for index_name in build_index_map[bucket_name]:
                        monitor_index_tasks.append(self.async_monitor_index(bucket_name, index_name))
                for task in monitor_index_tasks:
                    task.result()

    def test_multi_create_query_explain_drop_index(self):
        try:
            self._create_index_in_async()
            self.run_doc_ops()
            self._query_explain_in_async()
            self._verify_index_map()
        except Exception, ex:
            self.log.info(ex)
            raise
        finally:
            tasks = self.async_run_multi_operations(buckets=self.buckets, query_definitions=self.query_definitions,
                                                    drop_index=True)
            self._run_tasks(tasks)