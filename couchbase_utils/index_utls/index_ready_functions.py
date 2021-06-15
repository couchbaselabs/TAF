'''
Created on 07-May-2021

@author: riteshagarwal
'''
from global_vars import logger
import time
from membase.api.rest_client import RestConnection


class IndexUtils:

    def __init__(self, cluster, server_task, n1ql_node):
        self.cluster = cluster
        self.task = server_task
        self.task_manager = self.task.jython_task_manager
        self.n1ql_node = n1ql_node
        self.log = logger.get("test")

    def build_deferred_indexes(self, indexes_to_build):
        """
        Build secondary indexes that were deferred
        """
        self.log.info("Building indexes")
        for bucket, bucket_data in indexes_to_build.items():
            for scope, collection_data in bucket_data.items():
                for collection, gsi_index_names in collection_data.items():
                    build_query = "BUILD INDEX on `%s`.`%s`.`%s`(%s) " \
                                  "USING GSI" \
                                  % (bucket, scope, collection, gsi_index_names)
                    result = self.run_cbq_query(build_query)
                    self.assertTrue(result['status'] == "success", "Build query %s failed." % build_query)
                    self.wait_for_indexes_to_go_online(gsi_index_names)

        query = "select state from system:indexes where state='deferred'"
        result = self.run_cbq_query(query)
        self.log.info("deferred indexes remaining: {0}".format(len(result['results'])))
        query = "select state from system:indexes where state='online'"
        result = self.run_cbq_query(query)
        self.log.info("online indexes count: {0}".format(len(result['results'])))
        self.sleep(60, "Wait after building indexes")

    def create_indexes(self, buckets, gsi_base_name="gsi",
                       replica=0, defer=True):
        """
        Create gsi indexes on collections - according to number_of_indexes_per_coll
        """
        self.log.info("Creating indexes with defer:{} build".format(defer))
        indexes_to_build = dict()
        count = 0
        couchbase_buckets = [bucket for bucket in buckets
                             if bucket.bucketType == "couchbase"]
        for bucket in couchbase_buckets:
            indexes_to_build[bucket.name] = dict()
            for _, scope in bucket.scopes.items():
                indexes_to_build[bucket.name][scope.name] = dict()
                for _, collection in scope.collections.items():
                    for _ in range(self.number_of_indexes_per_coll):
                        gsi_index_name = gsi_base_name + str(count)
                        create_index_query = "CREATE INDEX `%s` " \
                                             "ON `%s`.`%s`.`%s`(`age`) " \
                                             "WITH { 'defer_build': %s, 'num_replica': %s }" \
                                             % (gsi_index_name, bucket.name,
                                                scope.name, collection.name,
                                                defer, replica)
                        result = self.run_cbq_query(create_index_query)
                        # self.assertTrue(result['status'] == "success", "Defer build Query %s failed." % create_index_query)

                        if collection.name not in indexes_to_build[bucket.name][scope.name]:
                            indexes_to_build[bucket.name][scope.name][collection.name] = list()
                        indexes_to_build[bucket.name][scope.name][collection.name].append(gsi_index_name)
                        count += 1
        return indexes_to_build

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

    def drop_indexes(self, num_indexes_to_drop=15):
        """
        Drop gsi indexes
        Returns dropped indexes dict
        """
        self.log.info("Dropping {0} indexes".format(num_indexes_to_drop))
        indexes_dropped = dict()
        count = 0
        for bucket, bucket_data in self.indexes_to_build.items():
            indexes_dropped[bucket] = dict()
            for scope, collection_data in bucket_data.items():
                indexes_dropped[bucket][scope] = dict()
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        drop_index_query = "DROP INDEX `%s` ON " \
                                           "`%s`.`%s`.`%s`" \
                                           "USING GSI" \
                                           % (gsi_index_name, bucket, scope, collection)
                        result = self.run_cbq_query(drop_index_query)
                        if collection not in indexes_dropped[bucket][scope]:
                            indexes_dropped[bucket][scope][collection] = list()
                        indexes_dropped[bucket][scope][collection].append(gsi_index_name)
                        count = count + 1
                        if count >= num_indexes_to_drop:
                            return indexes_dropped

    def run_cbq_query(self, query, n1ql_node=None, timeout=1300):
        """
        To run cbq queries
        Note: Do not run this in parallel
        """
        n1ql_node = n1ql_node or self.n1ql_node
        conn = RestConnection(n1ql_node)
        result = conn.query_tool(query, timeout)
        return result

    def wait_for_indexes_to_go_online(self, gsi_index_names, timeout=300):
        """
        Wait for indexes to go online after building the deferred indexes
        """
        self.log.info("Waiting for indexes to go online")
        start_time = time.time()
        stop_time = start_time + timeout
        for gsi_index_name in gsi_index_names:
            while True:
                check_state_query = "SELECT state FROM system:indexes WHERE name='%s'" % gsi_index_name
                result = self.run_cbq_query(check_state_query)
                if result['results'][0]['state'] == "online":
                    break
                if time.time() > stop_time:
                    self.fail("Index availability timeout of index: {0}".format(gsi_index_name))
