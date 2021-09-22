'''
Created on 07-May-2021

@author: riteshagarwal
'''
from global_vars import logger
import time
from membase.api.rest_client import RestConnection
from gsiLib.GsiHelper_Rest import GsiHelper
import string, random, math


class IndexUtils:

    def __init__(self, server_task=None):
        self.task = server_task
        self.task_manager = self.task.jython_task_manager
        self.log = logger.get("test")

    """
    Method to create list of RestClient for each node
    arguments: 
             a. nodes_list: list of nodes list
    return: list of restClient object corresponds each node in the list
    """

    def create_restClient_obj_list(self, nodes_list):
        restClient_list = list()
        for query_node in nodes_list:
            restClient_list.append(RestConnection(query_node))
        return restClient_list

    def build_deferred_indexes(self, cluster, indexes_to_build):
        """
        Build secondary indexes that were deferred
        """
        query_nodes_count = len(cluster.query_nodes)
        restClient_Obj_list = self.create_restClient_obj_list(cluster.query_nodes)
        self.log.info("Building indexes")
        x = 0
        for bucket, bucket_data in indexes_to_build.items():
            for scope, collection_data in bucket_data.items():
                for collection, gsi_index_names in collection_data.items():
                    build_query = "BUILD INDEX on `%s`.`%s`.`%s`(%s) " \
                                  "USING GSI" \
                                  % (bucket, scope, collection, gsi_index_names)
                    query_client = restClient_Obj_list[x % query_nodes_count]
                    query_client.query_tool(build_query)
                    x += 1

    def create_gsi_on_each_collection(self, cluster, buckets=None, gsi_base_name=None,
                                      replica=0, defer=True, number_of_indexes_per_coll=1, count=1,
                                      field='key', sync=False):
        """
        Create gsi indexes on collections - according to number_of_indexes_per_coll
        """
        self.log.info("Creating indexes with defer:{} build".format(defer))
        if buckets is None:
            buckets = cluster.buckets

        couchbase_buckets = [bucket for bucket in buckets
                             if bucket.bucketType == "couchbase"]
        query_node_list = cluster.query_nodes
        query_nodes_count = len(query_node_list)
        x = 0
        createIndexTasklist = list()
        indexes_to_build = dict()
        for bucket in couchbase_buckets:
            if bucket.name not in indexes_to_build:
                indexes_to_build[bucket.name] = dict()
            for _, scope in bucket.scopes.items():
                if scope.name not in indexes_to_build[bucket.name]:
                    indexes_to_build[bucket.name][scope.name] = dict()
                for _, collection in scope.collections.items():
                    for tempCount in range(count, number_of_indexes_per_coll):
                        if gsi_base_name is None:
                            gsi_index_name = bucket.name.replace(".", "") + "_" + scope.name + "_" +\
                                             collection.name + "_" + str(tempCount)
                        else:
                            gsi_index_name = gsi_base_name + str(tempCount)
                        create_index_query = "CREATE INDEX `%s` " \
                                             "ON `%s`.`%s`.`%s`(`%s`) " \
                                             "WITH { 'defer_build': %s, 'num_replica': %s }" \
                                             % (gsi_index_name, bucket.name,
                                                scope.name, collection.name, field,
                                                defer, replica)
                        query_node_instance = x % query_nodes_count
                        self.log.debug("sending query:"+create_index_query)
                        self.log.debug("Sending index name:"+gsi_index_name)
                        task = self.task.async_execute_query(server=query_node_list[query_node_instance],
                                                             query=create_index_query,
                                                             isIndexerQuery=True, bucket=bucket,
                                                             indexName=gsi_index_name)
                        if sync:
                            self.task_manager.get_task_result(task)
                        else:
                            createIndexTasklist.append(task)
                        if collection.name not in indexes_to_build[bucket.name][scope.name]:
                            indexes_to_build[bucket.name][scope.name][collection.name] = list()
                        indexes_to_build[bucket.name][scope.name][collection.name].append(gsi_index_name)

        return indexes_to_build, createIndexTasklist

    def recreate_dropped_indexes(self, indexes_dropped):
        """
        Recreate dropped indexes given indexes_dropped dict
        """
        self.log.info("Recreating dropped indexes")
        for bucket, bucket_data in indexes_dropped.items():
            for scope, collection_data in bucket_data.items():
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        create_index_query = "CREATE INDEX `%s` " \
                                             "ON `%s`.`%s`.`%s`(`age`)" \
                                             "WITH { 'defer_build': true, 'num_replica': 0 }" \
                                             % (gsi_index_name, bucket, scope, collection)
                        result = self.run_cbq_query(create_index_query)
        self.build_deferred_indexes(indexes_dropped)

    def async_drop_indexes(self, cluster, indexList, buckets=None):
        """
        Drop gsi indexes
        Returns dropped indexes dict and task list
        """
        indexes_dropped = dict()
        if buckets is None:
            buckets = cluster.buckets
        couchbase_buckets = [bucket for bucket in buckets
                             if bucket.bucketType == "couchbase"]
        query_nodes_list = cluster.query_nodes
        query_nodes_count = len(query_nodes_list)
        x = 0
        dropIndexTaskList = list()
        for bucket in couchbase_buckets:
            indexes_dropped[bucket.name] = dict()
            for _, scope in bucket.scopes.items():
                indexes_dropped[bucket.name][scope.name] = dict()
                for _, collection in scope.collections.items():
                    gsi_index_names = indexList[bucket.name][scope.name][collection.name]
                    for gsi_index_name in list(gsi_index_names):
                        drop_index_query = "DROP INDEX `%s` ON " \
                                           "`%s`.`%s`.`%s`" \
                                           "USING GSI" \
                                           % (gsi_index_name, bucket, scope, collection)
                        query_node_index = x % query_nodes_count
                        task = self.task.async_execute_query(server=query_nodes_list[query_node_index],
                                                             query=drop_index_query,
                                                             bucket=bucket,
                                                             indexName=gsi_index_name, isIndexerQuery=False)
                        dropIndexTaskList.append(task)
                        gsi_index_names.remove(gsi_index_name)
                        if collection.name not in indexes_dropped[bucket.name][scope.name]:
                            indexes_dropped[bucket.name][scope.name][collection.name] = list()
                        indexes_dropped[bucket.name][scope.name][collection.name].append(gsi_index_name)
                        x += 1
        return dropIndexTaskList, indexes_dropped

    def run_cbq_query(self, query, n1ql_node=None, timeout=1300):
        """
        To run cbq queries
        Note: Do not run this in parallel
        """
        n1ql_node = n1ql_node or self.n1ql_node
        conn = RestConnection(n1ql_node)
        result = conn.query_tool(query, timeout)
        return result

    def wait_for_indexes_to_go_online(self, cluster, buckets, gsi_index_name, timeout=300):
        """
        Wait for indexes to go online after building the deferred indexes
        """
        self.log.info("Waiting for indexes to go online")
        start_time = time.time()
        stop_time = start_time + timeout
        self.indexer_rest = GsiHelper(cluster.master, self.log)
        for bucket in buckets:
            if gsi_index_name.find(bucket.name.replace(".", "")) > -1:
                while True:
                    if self.indexer_rest.polling_create_index_status(bucket=bucket, index=gsi_index_name) is True:
                        return True
                    else:
                        if time.time() > stop_time:
                            return False

    def randStr(self, Num=10):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(Num))

    def run_full_scan(self, cluster, indexesDict, key):
        x = 0
        query_tasks_info = list()
        x = 0
        query_len = len(cluster.query_nodes)
        for bucket, bucket_data in indexesDict.items():
            for scope, collection_data in bucket_data.items():
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        query_node_index = x % query_len
                        queryString = self.randStr(Num=8)
                        query = "select * from `%s`.`%s`.`%s` data USE INDEX (%s USING GSI) where %s is not missing;" % (
                            bucket, scope, collection, gsi_index_name, key)
                        task = self.task.async_execute_query(cluster.query_nodes[query_node_index], query)
                        query_tasks_info.append(task)
        return query_tasks_info
