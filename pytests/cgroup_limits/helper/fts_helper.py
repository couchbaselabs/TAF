from cb_server_rest_util.search.search_api import SearchRestAPI
from fts_utils.fts_ready_functions import FTSUtils
from cb_constants import CbServer
import threading
import time


class CGroupFTSHelper:

    def __init__(self, cluster, task, logger):
        self.cluster = cluster
        self.fts_node = self.cluster.fts_nodes[0]
        self.task = task
        self.log = logger
        self.log.info("FTS node: {}".format(self.fts_node.__dict__))

    def create_fts_indexes(self):

        self.fts_indexes = []
        fts_idx_prefix = "test_fts_index"
        self.fts_index_count = 0
        fts_util = FTSUtils(self.cluster, self.task)
        for bucket in self.cluster.buckets:
            scopes_keys = bucket.scopes.keys()
            for scope in scopes_keys:
                if scope == CbServer.system_scope:
                    continue
                collections_keys = bucket.scopes[scope].collections.keys()
                for collection in collections_keys:
                    fts_idx_name = fts_idx_prefix+str(self.fts_index_count)
                    self.log.info("Creating FTS index: {} on {}:{}".format(
                                    fts_idx_name, scope, collection))
                    status = fts_util.create_fts_indexes(bucket, scope,
                                        collection, fts_idx_name)
                    self.log.info("Result of creation of fts index: " \
                                    "{0} = {1}".format(fts_idx_name, status))
                    self.fts_index_count += 1
                    self.fts_indexes.append(fts_idx_name)
                    time.sleep(5)

    def fts_execute_query(self, idx_name, iter=1):

        fts_query_template = {
            "explain": True,
            "fields": [
                "*"
            ],
            "highlight": {},
            "query": {
                "query": "Burgundy"
            },
            "size": 10,
            "from": 0
        }

        fts_query_template = str(fts_query_template).replace("True", "true")
        fts_query_template = str(fts_query_template).replace("False", "false")
        fts_query_template = str(fts_query_template).replace("'", "\"")

        fts_client = SearchRestAPI(self.fts_node)
        for i in range(iter):
            status, content = fts_client.run_fts_query(idx_name, str(fts_query_template))
            self.log.info("Result of querying FTS index: {} = {}".format(idx_name, status))

        self.log.info("Content = {}".format(content))

    def run_concurrent_fts_queries(self):

        thread_array = list()
        for fts_index in self.fts_indexes:
            th = threading.Thread(target=self.fts_execute_query, args=[fts_index, 30])
            th.start()
            thread_array.append(th)

        for th in thread_array:
            th.join()