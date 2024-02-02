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

from cb_constants.CBServer import CbServer
from FtsLib.FtsOperations import FtsHelper
from TestInput import TestInputSingleton
from aGoodDoctor.serverlessfts import ftsQueries, ftsIndex, HotelQueries, \
    HotelIndex, template
from com.couchbase.client.core.error import TimeoutException, \
    AmbiguousTimeoutException, UnambiguousTimeoutException, \
    RequestCanceledException, CouchbaseException
from com.couchbase.client.java.search import SearchQuery
from global_vars import logger
from java.net import SocketTimeoutException
from elasticsearch import EsClient
from com.couchbase.test.val import Vector
try:
    from vectorSearch.vectorFTS import predictor, vector
except:
    pass


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

vectorIndex = {
    "dynamic": False,
    "enabled": True,
    "properties": {
        "embedding": {
            "dynamic": False,
            "enabled": True,
            "fields": [
                {
                    "dims": 384,
                    "index": True,
                    "name": "embedding",
                    "similarity": "l2_norm",
                    "type": "vector"
                    }
                ]
            }
        }
    }


class DoctorFTS:

    def __init__(self, bucket_util):
        self.bucket_util = bucket_util
        self.input = TestInputSingleton.input
        self.fts_index_partitions = self.input.param("fts_index_partition", 8)
        self.log = logger.get("test")
        self.indexes = dict()
        self.stop_run = False

    def create_fts_indexes(self, cluster, dims=1536, similarity="l2_norm"):
        status = False
        fts_helper = FtsHelper(cluster.fts_nodes[0])
        for b in cluster.buckets:
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
                    if valType == "Vector":
                        indexType = vectorIndex
                        indexType["properties"]["embedding"]["fields"][0].update({"dims": dims,
                                                                                "similarity": similarity})
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
                            status, content = fts_helper.create_fts_index_from_json(
                                name, str(fts_param_template))
                            if content.find(" an index with the same name already exists") != -1:
                                status = True
                            if status is False:
                                self.log.critical("FTS index creation failed")
                                time.sleep(10)
                                retry -= 1
                            else:
                                b.ftsIndexes.update({name: (c, queryTypes)})
                                break
                        i += 1
                        time.sleep(1)

    def discharge_FTS(self):
        self.stop_run = True

    def wait_for_fts_index_online(self, cluster, timeout=86400):
        fts_helper = FtsHelper(cluster.fts_nodes[0])
        status = False
        for bucket in cluster.buckets:
            for index_name, _ in bucket.ftsIndexes.items():
                status = False
                stop_time = time.time() + timeout
                while time.time() < stop_time:
                    _status, content = fts_helper.fts_index_item_count(
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

    def drop_fts_indexes(self, cluster, idx_name):
        """
        Drop count number of fts indexes using fts name
        from fts_dict
        """
        fts_helper = FtsHelper(cluster.fts_nodes[0])
        self.log.debug("Dropping fts index: {}".format(idx_name))
        status, _ = fts_helper.delete_fts_index(idx_name)
        return status


class FTSQueryLoad:
    def __init__(self, cluster, bucket, esClient=None, mockVector=None, dim=None):
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
        self.cluster = cluster
        self.cluster_conn = random.choice(self.bucket.clients).cluster
        self.fts_node = random.choice(self.cluster.fts_nodes)
        self.fts_helper = FtsHelper(self.fts_node)
        self.esClient = esClient
        self.mockVector = mockVector
        self.dim = dim

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
        method = self._run_query
        if self.bucket.loadDefn.get("valType") == "Vector":
            method = self._run_vector_query
        query_count = 0
        for i in range(0, self.concurrent_queries_to_run):
            threads.append(Thread(
                target=method,
                name="query_thread_{0}".format(self.bucket.name + str(i)),
                args=()))

        for thread in threads:
            thread.start()
            query_count += 1

        for thread in threads:
            thread.join()

    def _run_vector_query(self):
        while not self.stop_run:
            index, _tuple = random.choice(self.bucket.ftsIndexes.items())
            collection, queries = _tuple
            query = random.choice(queries)
            k = random.randint(2, 50)
            query = {"query": {"match_none": {}}, "explain": False, "knn":
                     [{"field": "embedding", "k": k,
                       "vector": []}], "size": k}
            text_options = random.choice(([vector.colors, vector.clothingType, vector.fashionBrands],
                                 [vector.colors, vector.clothingType],
                                 [vector.clothingType, vector.fashionBrands]))
            text = ""
            flt_buf = Vector.flt_buf
            text = ""
            vector_float = []
            if self.mockVector:
                _slice = random.randint(0, Vector.flt_buf_length-self.dim)
                embedding = flt_buf[_slice: _slice+self.dim]
            else:
                for option in text_options:
                    text += random.choice(option) + " "
                text_vector = predictor.predict(text)
                embedding = text_vector.tolist()
            vector_float = []
            for value in embedding:
                vector_float.append(float(value))
            query["knn"][0].update({"vector": vector_float})
            query = json.dumps(query)
            start = time.time()
            e = ""
            try:
                self.total_query_count += 1
                if self.fts_node not in self.cluster.fts_nodes:
                    self.fts_node = random.choice(self.cluster.fts_nodes)
                    self.fts_helper = FtsHelper(self.fts_node)
                query = str(query).replace("True", "true")
                query = str(query).replace("False", "false")
                status, result = self.fts_helper.run_fts_query_curl(index, query)
                result = json.loads(str(result).encode().decode())
                if status:
                    if result["status"].get("errors"):
                        self.error_count.next()
                        self.log.critical(result["status"]["errors"])
                    elif k == result["total_hits"]:
                        self.success_count.next()
                        if self.esClient:
                            try:
                                esResult = self.esClient.performKNNSearch(collection.lower().replace("_", ""), "embedding", text_vector, k);
                            except SocketTimeoutException as e:
                                self.esClient = EsClient(self.esClient.serverUrl, self.esClient.apiKey)
                                self.esClient.initializeSDK()
                                print e
                            esResult = json.loads(str(esResult).encode().decode())
                            accuracy = 0
                            recall = 0
                            all_ids_elastic = list()
                            for hit in esResult["hits"]["hits"]:
                                all_ids_elastic.append(hit["_id"])
                            for index, item in enumerate(result["hits"]):
                                if item['id'] == all_ids_elastic[index]:
                                    accuracy += 1
                                if item['id'] in all_ids_elastic:
                                    recall += 1
                            self.log.critical("Query: {}, k={}, Accuracy: {}, recall: {}".format(
                                text, k, round(100.0 * accuracy/k, 2), round(100.0 * recall/k, 2)))
                    else:
                        self.failed_count.next()
                        self.log.critical("k=%s, total_hits=%s, hits=%s" % (k, result["total_hits"], len(result["hits"])))
            except TimeoutException or AmbiguousTimeoutException or UnambiguousTimeoutException as e:
                pass
            except RequestCanceledException as e:
                pass
            except CouchbaseException as e:
                pass
            except Exception as e:
                print e
            if str(e).find("TimeoutException") != -1\
                or str(e).find("AmbiguousTimeoutException") != -1\
                    or str(e).find("UnambiguousTimeoutException") != -1:
                self.timeout_count.next()
            elif str(e).find("RequestCanceledException") != -1:
                self.failures += self.cancel_count.next()
            elif str(e).find("CouchbaseException") != -1:
                self.failures += self.error_count.next()

            if str(e).find("no more information available") != -1:
                self.log.critical(query)
                self.log.critical(e)
            end = time.time()
            if end - start < 1:
                time.sleep(end - start)


    def _run_query(self, validate_item_count=False, expected_count=0):
        while not self.stop_run:
            index, _tuple = random.choice(self.bucket.ftsIndexes.items())
            _, queries = _tuple
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
                self.failures += self.error_count.next()

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
