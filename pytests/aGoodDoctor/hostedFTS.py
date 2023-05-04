'''
Created on May 31, 2022

@author: ritesh.agarwal
'''

from FtsLib.FtsOperations import FtsHelper
from global_vars import logger
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection
import time
import json
from Cb_constants.CBServer import CbServer
from table_view import TableView
import random
from com.couchbase.client.java.search import SearchQuery
import itertools
from sdk_client3 import SDKClient
import threading
from threading import Thread
from com.couchbase.client.core.error import TimeoutException,\
    AmbiguousTimeoutException, UnambiguousTimeoutException,\
    RequestCanceledException, CouchbaseException, PlanningFailureException

ftsQueries = [
            SearchQuery.queryString("pJohn"),
            SearchQuery.match("pJohn"),
            SearchQuery.prefix("Cari")
            ]

HotelQueries = [
            SearchQuery.queryString("United Kingdom"),
            SearchQuery.match("Algeria"),
            SearchQuery.prefix("Serbi"),
    ]


class DoctorFTS:

    def __init__(self, cluster, bucket_util):
        self.cluster = cluster
        self.bucket_util = bucket_util
        self.input = TestInputSingleton.input
        self.fts_index_partitions = self.input.param("fts_index_partition", 1)
        self.log = logger.get("test")
        self.fts_helper = FtsHelper(self.cluster.fts_nodes[0])
        self.indexes = dict()
        self.stop_run = False

    def create_fts_indexes(self, buckets):
        status = False
        for b in buckets:
            b.FTSindexes = dict()
            i = 0
            while i < b.loadDefn.get("FTS")[0]:
                for s in self.bucket_util.get_active_scopes(b, only_names=True):
                    for c in sorted(self.bucket_util.get_active_collections(b, s, only_names=True)):
                        if c == CbServer.default_collection:
                            continue
                        fts_param_template = self.get_fts_idx_template()
                        fts_param_template.update({
                            "name": "fts_idx_{}".format(i), "sourceName": b.name})
                        fts_param_template["planParams"].update({
                            "indexPartitions": self.fts_index_partitions})
                        fts_param_template["params"]["mapping"]["types"].update({
                            "%s.%s" % (s, c): {
                                "dynamic": True, "enabled": True}
                            }
                        )
                        fts_param_template = str(fts_param_template).replace("True", "true")
                        fts_param_template = str(fts_param_template).replace("False", "false")
                        fts_param_template = str(fts_param_template).replace("'", "\"")
                        name = "fts_idx_"+str(i)
                        index_tuple = (fts_param_template, b.name, s, c)
                        b.FTSindexes.update({name: index_tuple})
                        retry = 5
                        status = False
                        while not status and retry > 0:
                            self.log.debug("Creating fts index: {} on {}.{}".format(name, b.name, c))
                            try:
                                status, _ = self.fts_helper.create_fts_index_from_json(
                                    name, str(index_tuple[0]))
                            except PlanningFailureException or CouchbaseException or UnambiguousTimeoutException or TimeoutException or AmbiguousTimeoutException or RequestCanceledException as e:
                                print(e)
                                time.sleep(10)
                            retry -= 1
                        i += 1

    def discharge_FTS(self):
        self.stop_run = True

    def get_fts_idx_template(self):
        fts_idx_template = {
            "type": "fulltext-index",
            "name": "fts-index",
            "sourceType": "gocbcore",
            "sourceName": "default",
            "planParams": {
                "maxPartitionsPerPIndex": 1024,
                "indexPartitions": 1,
                "numReplicas": 1
             },
            "params": {
                "doc_config": {
                    "docid_prefix_delim": "",
                    "docid_regexp": "",
                    "mode": "scope.collection.type_field",
                    "type_field": "type"
                    },
                "mapping": {
                    "analysis": {},
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "dynamic": True,
                        "enabled": False
                        },
                    "default_type": "_default",
                    "docvalues_dynamic": False,
                    "index_dynamic": True,
                    "store_dynamic": False,
                    "type_field": "_type",
                    "types": {}
                    },
                "store": {
                    "indexType": "scorch",
                    "segmentVersion": 15
                    }
                },
            "sourceParams": {}
           }
        return fts_idx_template

    def wait_for_fts_index_online(self, buckets, timeout=86400):
        status = False
        for bucket in buckets:
            for index_name, details in bucket.FTSindexes.items():
                status = False
                stop_time = time.time() + timeout
                while time.time() < stop_time:
                    _status, content = self.fts_helper.fts_index_item_count(
                        "%s" % (index_name))
                    self.log.debug("index: {}, status: {}, count: {}, expected: {}"
                                   .format(index_name, _status,
                                           json.loads(content)["count"], bucket.loadDefn.get("num_items")))
                    if json.loads(content)["count"] == bucket.loadDefn.get("num_items"):
                        self.log.info("FTS index is ready: {}".format(index_name))
                        status = True
                        break
                    time.sleep(5)
                if status is False:
                    return status
        return status

    def drop_fts_indexes(self, idx_name):
        """
        Drop count number of fts indexes using fts name
        from fts_dict
        """
        self.log.debug("Dropping fts index: {}".format(idx_name))
        status, _ = self.fts_helper.delete_fts_index(idx_name)
        return status


class FTSQueryLoad:
    def __init__(self, bucket):
        self.bucket = bucket
        self.failed_count = itertools.count()
        self.success_count = itertools.count()
        self.rejected_count = itertools.count()
        self.error_count = itertools.count()
        self.cancel_count = itertools.count()
        self.timeout_count = itertools.count()
        self.total_query_count = 0
        self.stop_run = False
        self.cluster_conn = SDKClient([bucket.serverless.nebula_endpoint], None).cluster
        self.log = logger.get("infra")

    def start_query_load(self):
        th = threading.Thread(target=self._run_concurrent_queries)
        th.start()

    def stop_query_load(self):
        self.stop_run = True
        try:
            if self.cluster_conn:
                self.cluster_conn.close()
        except:
            pass

    def _run_concurrent_queries(self):
        threads = []
        self.total_query_count = 0
        self.concurrent_queries_to_run = self.bucket.loadDefn.get("FTS")[1]
        self.currently_running = 0
        query_count = 0
        for i in range(0, self.concurrent_queries_to_run):
            threads.append(Thread(
                target=self._run_query,
                name="query_thread_{0}".format(self.bucket.name + str(i)),
                args=()))

        for thread in threads:
            thread.start()
            query_count += 1

        for thread in threads:
            thread.join()

        if self.failed_count.next()-1 > 0 or self.error_count.next()-1 > 0:
            raise Exception("Queries Failed:%s , Queries Error Out:%s" %
                            (self.failed_count, self.error_count))

    def _run_query(self, validate_item_count=False, expected_count=0):
        while not self.stop_run:
            query = random.choice(ftsQueries)
            if self.bucket.loadDefn.get("valType") == "Hotel":
                    query = random.choice(HotelQueries)
            index, _ = random.choice(self.bucket.ftsIndexes.items())
            start = time.time()
            e = ""
            try:
                self.total_query_count += 1
                result = self.execute_fts_query("{}".format(index), query)
                if validate_item_count:
                    if result.metaData().metrics().totalRows() != expected_count:
                        self.failed_count.next()
                    else:
                        self.success_count.next()
                else:
                    self.success_count.next()
            except TimeoutException or AmbiguousTimeoutException or UnambiguousTimeoutException as e:
                pass
            except RequestCanceledException as e:
                pass
            except CouchbaseException as e:
                pass
            except Exception as e:
                pass
            if str(e).find("TimeoutException") != -1\
                or str(e).find("AmbiguousTimeoutException") != -1\
                    or str(e).find("UnambiguousTimeoutException") != -1:
                self.timeout_count.next()
            elif str(e).find("RequestCanceledException") != -1:
                self.cancel_count.next()
            elif str(e).find("CouchbaseException") != -1:
                self.rejected_count.next()

            if str(e).find("no more information available") != -1:
                self.log.critical(query)
                self.log.critical(e)
            end = time.time()
            if end - start < 1:
                time.sleep(end - start)

    def execute_fts_query(self, index, query):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        result = self.cluster_conn.searchQuery(index, query)
        return result

    def monitor_query_status(self, print_duration=600):
        st_time = time.time()
        while not self.stop_run:
            if st_time + print_duration < time.time():
                self.table = TableView(self.log.info)
                self.table.set_headers(["Bucket",
                                        "Total Queries",
                                        "Failed Queries",
                                        "Success Queries",
                                        "Rejected Queries",
                                        "Cancelled Queries",
                                        "Timeout Queries",
                                        "Errored Queries"])
                self.table.add_row([
                    str(self.bucket.name),
                    str(self.total_query_count),
                    str(self.failed_count),
                    str(self.success_count),
                    str(self.rejected_count),
                    str(self.cancel_count),
                    str(self.timeout_count),
                    str(self.error_count),
                    ])
                self.table.display("FTS Query Statistics")
                st_time = time.time()
