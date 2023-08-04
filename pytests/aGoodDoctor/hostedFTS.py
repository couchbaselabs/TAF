'''
Created on May 31, 2022

@author: ritesh.agarwal
'''

from copy import deepcopy
import itertools
import json
import random
from threading import Thread
import threading
import time

from Cb_constants.CBServer import CbServer
from FtsLib.FtsOperations import FtsHelper
from TestInput import TestInputSingleton
from aGoodDoctor.serverlessfts import ftsQueries, ftsIndex, HotelQueries, \
    HotelIndex, template
from com.couchbase.client.core.error import TimeoutException, \
    AmbiguousTimeoutException, UnambiguousTimeoutException, \
    RequestCanceledException, CouchbaseException
from com.couchbase.client.java.search import SearchQuery
from global_vars import logger


NimbusPQueries = [
            SearchQuery.queryString("000000000000000000000000000000406101"),
            SearchQuery.match("Bhutan"),
            SearchQuery.prefix("Zim"),
    ]

NimbusMQueries = [
            SearchQuery.queryString("uWKyrYzYhD"),
            SearchQuery.match("000000000000000000000000000000833238"),
            SearchQuery.prefix("0000"),
    ]


class DoctorFTS:

    def __init__(self, cluster, bucket_util):
        self.cluster = cluster
        self.bucket_util = bucket_util
        self.input = TestInputSingleton.input
        self.fts_index_partitions = self.input.param("fts_index_partition", 8)
        self.log = logger.get("test")
        self.fts_helper = FtsHelper(self.cluster.fts_nodes[0])
        self.indexes = dict()
        self.stop_run = False

    def create_fts_indexes(self, buckets):
        status = False
        for b in buckets:
            b.ftsIndexes = dict()
            b.FTSqueries = ftsQueries
            b.ftsIndexes
            i = 0
            for s in self.bucket_util.get_active_scopes(b, only_names=True):
                if s == CbServer.system_scope:
                    continue
                for collection_num, c in enumerate(sorted(self.bucket_util.get_active_collections(b, s, only_names=True))):
                    if c == CbServer.default_collection:
                        continue
                    workloads = b.loadDefn.get("collections_defn", [b.loadDefn])
                    workload = workloads[collection_num % len(workloads)]
                    valType = workload["valType"]
                    queryTypes = ftsQueries
                    indexType = ftsIndex
                    if valType == "Hotel":
                        queryTypes = HotelQueries
                        indexType = HotelIndex
                    if valType == "NimbusP":
                        queryTypes = NimbusPQueries
                    if valType == "NimbusM":
                        queryTypes = NimbusMQueries
                    i = 0
                    while i < workload.get("FTS")[0]:
                        name = str(b.name).replace("-", "_") + c + "_fts_idx_"+str(i)
                        fts_param_template = deepcopy(template)
                        fts_param_template.update({
                            "name": name, "sourceName": str(b.name)})
                        fts_param_template["planParams"].update({
                            "indexPartitions": self.fts_index_partitions})
                        fts_param_template["params"]["mapping"]["types"].update({"%s.%s" % (s, c): indexType})
                        fts_param_template = str(fts_param_template).replace("True", "true")
                        fts_param_template = str(fts_param_template).replace("False", "false")
                        fts_param_template = str(fts_param_template).replace("'", "\"")
                        self.log.debug("Creating fts index: {}".format(name))
                        retry = 10
                        while retry > 0:
                            status, content = self.fts_helper.create_fts_index_from_json(
                                name, str(fts_param_template))
                            if content.find(" an index with the same name already exists") != -1:
                                status = True
                            if status is False:
                                self.log.critical("FTS index creation failed")
                                time.sleep(10)
                                retry -= 1
                            else:
                                b.ftsIndexes.update({name: (queryTypes)})
                                break
                        i += 1
                        time.sleep(1)

    def discharge_FTS(self):
        self.stop_run = True

    def wait_for_fts_index_online(self, buckets, timeout=86400):
        status = False
        for bucket in buckets:
            for index_name, _ in bucket.ftsIndexes.items():
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
        self.log = logger.get("infra")
        self.failures = 0

    def start_query_load(self):
        th = threading.Thread(target=self._run_concurrent_queries)
        th.start()

    def stop_query_load(self):
        self.stop_run = True

    def _run_concurrent_queries(self):
        threads = []
        self.total_query_count = 0
        self.concurrent_queries_to_run = self.bucket.loadDefn.get("ftsQPS")
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
            index, queries = random.choice(self.bucket.ftsIndexes.items())
            query = random.choice(queries)
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
                self.failures += self.cancel_count.next()
            elif str(e).find("CouchbaseException") != -1:
                self.failures += self.rejected_count.next()

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
        result = random.choice(self.bucket.clients).cluster.searchQuery(index, query)
        return result
