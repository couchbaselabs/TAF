import threading
import time
from cb_server_rest_util.query.query_api import QueryRestAPI
from cb_constants import CbServer


class CGroupQueryHelper:

    def __init__(self, cluster, logger):

        self.cluster = cluster
        self.query_node = self.cluster.query_nodes[0]
        self.log = logger
        self.log.info("Query node: {}".format(self.query_node.__dict__))

    def create_secondary_indexes(self):
        query_client = QueryRestAPI(self.query_node)
        indexes = [
           'CREATE INDEX `{}` on `{}`.`{}`.`{}`(age) where age between 30 and 50;',

           'CREATE INDEX `{}` on`{}`.`{}`.`{}`(marital,age);',

           'CREATE INDEX `{}` on `{}`.`{}`.`{}`(ALL `animals`,`attributes`.`hair`,`name`) '
           'where attributes.hair = "Burgundy";',

           'CREATE INDEX `{}` ON `{}`.`{}`.`{}`(`gender`,`attributes`.`hair`, DISTINCT ARRAY '
           '`hobby`.`type` FOR hobby in `attributes`.`hobbies` END) where gender="F" '
           'and attributes.hair = "Burgundy";',

           'CREATE INDEX `{}` on `{}`.`{}`.`{}`(`gender`,`attributes`.`dimensions`.`weight`, '
           '`attributes`.`dimensions`.`height`,`name`);'
        ]

        idx_prefix = "new_index"
        count = 0
        for bucket in self.cluster.buckets:
            self.log.info(f"Loading data to bucket:{bucket.name}")
            scopes_keys = bucket.scopes.keys()
            for scope in scopes_keys:
                if scope == CbServer.system_scope:
                    continue
                collections_keys = bucket.scopes[scope].collections.keys()
                for collection in collections_keys:
                    self.log.info("Creating indexes for {}:{}:{}".format(bucket.name, scope, collection))
                    for idx_query in indexes:
                        idx_name = idx_prefix + str(count)
                        idx_query = idx_query.format(idx_name, bucket.name, scope, collection)
                        self.log.info("Index query = {}".format(idx_query))
                        status, content = query_client.run_query(params={"statement": idx_query}, timeout=2400)
                        self.log.info("Result for creation of {0} = {1}, {2}".format(idx_name, status, content))
                        count += 1

    def query_tool(self, server, query, iter=30, timeout=300):

        query_client = QueryRestAPI(server)

        for i in range(iter):
            status, content = query_client.run_query(params={"statement": query}, timeout=timeout)
            self.log.info("Result of query iter: {} = {}".format(i, status))
            if not status:
                self.log.info("Error during query = {}".format(content))

    def run_queries(self, num_clients=1, iter=1):

        # query_node = self.node_service_dict["query"][0]
        self.log.info("Query node: {}".format(self.query_node.__dict__))

        complex_queries = [
            'select name from `{}`.`{}`.`{}` '
            'where age between 30 and 50 limit 20000;',

            'select age, count(*) from `{}`.`{}`.`{}` '
            'where marital = "M" group by age order by age limit 20000;',

            'select v.name, animal from `{}`.`{}`.`{}` as v '
            'unnest animals as animal where v.attributes.hair="Burgundy" '
            'limit 10000;',

            'SELECT v.name, ARRAY hobby.name FOR hobby '
            'IN v.attributes.hobbies END FROM `{}`.`{}`.`{}` '
            'as v WHERE v.attributes.hair = "Burgundy" and gender = "F" '
            'and ANY hobby IN v.attributes.hobbies '
            'SATISFIES hobby.type="Music" END limit 10000;',

            'select name, ROUND'
            '(attributes.dimensions.weight/attributes.dimensions.height,2) '
            'from `{}`.`{}`.`{}` '
            'WHERE gender is not MISSING limit 20000;']

        recursive_cte_query = "WITH RECURSIVE cte AS ( " \
                            " SELECT 1 AS r " \
                            " UNION " \
                            " SELECT cte.r+1 AS r " \
                            " FROM cte " \
                            ")" \
                            " SELECT cte.r FROM cte;"

        thread_array = list()
        num_client_per_coll = num_clients // (len(self.cluster.buckets[0].scopes.keys()) \
                            * len(self.cluster.buckets[0].scopes["_default"].collections.keys()))

        for bucket in self.cluster.buckets:
            scopes_keys = bucket.scopes.keys()
            for scope in scopes_keys:
                if scope == CbServer.system_scope:
                    continue
                collections_keys = bucket.scopes[scope].collections.keys()
                for collection in collections_keys:
                    self.log.info("Running SELECT queries on {}:{}:{}".format(
                                    bucket.name, scope, collection))
                    for query in complex_queries:
                        c = num_client_per_coll
                        self.log.info("Query = {}, Num Clients = {}, Iterations = {}".\
                                      format(query, num_client_per_coll, iter))
                        query = query.format(bucket.name, scope, collection)
                        while c > 0:
                            th = threading.Thread(target=self.query_tool,
                                                  args=[self.query_node, query, iter])
                            th.start()
                            thread_array.append(th)
                            c -= 1

        self.log.info("Sleeping before running recursive CTE query")
        time.sleep(60)
        th = threading.Thread(target=self.query_tool,
                              args=[self.query_node, recursive_cte_query, 2, 1000])
        th.start()
        thread_array.append(th)

        for th in thread_array:
            th.join()