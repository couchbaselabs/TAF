'''
Created on 24-Apr-2021

@author: riteshagarwal
'''

import json
import random
import re
from threading import Thread
import threading
import time
import traceback

from string import ascii_uppercase, ascii_lowercase, digits
from gsiLib.gsiHelper import GsiHelper
from global_vars import logger
from collections import defaultdict
from table_view import TableView
import itertools
from py_constants.cb_constants.CBServer import CbServer
from connections.Rest_Connection import RestConnection
import struct
from .workloads import siftBigANN
from .workload_template import TEMPLATE_KEY
import os
from threading import Lock
from TestInput import TestInputSingleton
import faker
from couchbase.n1ql import QueryScanConsistency, QueryStatus
from couchbase.exceptions import (UnAmbiguousTimeoutException, CouchbaseException, TimeoutException, AmbiguousTimeoutException,
                                  RequestCanceledException, InternalServerFailureException, QueryIndexAlreadyExistsException)
from couchbase.options import QueryOptions
from elasticsearch import Elasticsearch, ConnectionTimeout

letters = ascii_uppercase + ascii_lowercase + digits

_PLAIN_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def n1ql_identifier(name):
    """
    `name` ready for a bucket.scope.collection path. A plain identifier is
    returned as-is; anything else is backtick-escaped. Workload template
    collections are named after their collection_id, and ids like "8M" start
    with a digit, which N1QL only accepts escaped. Every name the val_type
    workloads create is a plain identifier, so their statements don't change.
    """
    return name if _PLAIN_IDENTIFIER.match(name) else f"`{name}`"


def execute_statement_on_n1ql(client, statement, client_context_id=None,
                              query_params=None, validate=True):
    """
    Executes a statement on CBAS using the REST API using REST Client
    """
    response = execute_via_sdk(client, statement, False, client_context_id, query_params, validate)
    if type(response) == str:
        response = json.loads(response)
    if "errors" in response:
        errors = response["errors"]
    else:
        errors = None

    if validate and "results" in response:
        results = response["results"]
    else:
        results = None

    if "handle" in response:
        handle = response["handle"]
    else:
        handle = None

    if "metrics" in response:
        metrics = response["metrics"]
    else:
        metrics = None
    if "status" in response:
        status = response["status"]
    else:
        status = None
    return status, metrics, errors, results, handle


def execute_via_sdk(client, statement, readonly=False, client_context_id=None, query_params=None, validate=True):
    params = dict()
    if query_params:
        for k, v in query_params.items():
            try:
                params.update({k: eval(v)})
            except:
                params.update({k: v})
    options = QueryOptions(metrics=True,
                           client_context_id=client_context_id,
                           named_parameters=params)
    output = {}

    result = client.query(statement, options)
    try:
        # Always drain the row stream so the query fully executes and its
        # metadata (status/metrics/errors) is available. Retain the rows only
        # when the caller wants to validate them (e.g. item-count checks) -
        # otherwise we still get metrics/status without holding the results.
        # This keeps metrics from being None when validate=False (e.g. MSMARCO,
        # which has no ground truth and disables result validation).
        rows = [row for row in result.rows()]
        if validate:
            output["results"] = rows
        output["status"] = result.metadata().status()
        output["metrics"] = result.metadata().metrics()
        output["errors"] = result.metadata().errors()
        if str(output['status']) == QueryStatus.FATAL:
            msg = output['errors'][0]['msg']
            if "Job requirement" in msg and "exceeds capacity" in msg:
                raise Exception("Capacity cannot meet job requirement")
    except CouchbaseException as e:
        raise e

    return output

def performKNNSearchES(esClient, collection, qVec, k, gt, es_params=None):
    try:
        esResult = esClient.search(
            index=collection.lower().replace("_", ""),
            body={
                "query": {
                    "knn": {
                        "field": "embedding",
                        "query_vector": qVec,
                        "k": k,
                        "num_candidates": 1000
                    }
                },
                "size": k,
                **(es_params or {})
            }
        )
    except ConnectionTimeout as e:
        print(e)
        esClient = Elasticsearch(esClient.transport.hosts)
        esResult = esClient.search(
            index=collection.lower().replace("_", ""),
            body={
                "query": {
                    "knn": {
                        "field": "embedding",
                        "query_vector": qVec,
                        "k": k,
                        "num_candidates": 1000
                    }
                },
                "size": k,
                **(es_params or {})
            }
        )

    esResult = esResult.to_dict()
    recall = 0
    all_ids_elastic = [hit["_source"]["id"] for hit in esResult["hits"]["hits"]]
    for item in all_ids_elastic:
        if item in gt[:100]:
            recall += 1
    return round(100.0 * recall / k, 2), esResult["took"]

class DoctorN1QL():

    def __init__(self, bucket_util):
        self.bucket_util = bucket_util
        self.sdkClients = dict()
        self.log = logger.get("test")
        self.stop_run = False
        self.query_failure = False

    def create_indexes(self, buckets, skip_index=False, base64=False, xattr=False,
                       combinational=False):
        for b in buckets:
            # counter = 0
            b.indexes = dict()
            b.queries = list()
            b.query_map = dict()
            vector_defn_counter = 0
            # A workload template collection is named after its definition
            template_defns = None
            if b.loadDefn.get(TEMPLATE_KEY):
                template_defns = {defn["collection_id"]: defn
                                  for defn in b.loadDefn["collections_defn"]}
            self.log.info("Creating GSI indexes on {}".format(b.name))
            # prev_valType = None
            for s in self.bucket_util.get_active_scopes(b, only_names=True):
                if s == CbServer.system_scope:
                    continue
                if b.name+s not in self.sdkClients.keys():
                    self.sdkClients.update({b.name+s: b.clients[0].cluster})
                    time.sleep(1)
                collections = list(b.scopes[s].collections.keys())
                if "_default" in collections:
                    collections.remove("_default")
                for collection_num, c in enumerate(sorted(collections)):
                    if c == CbServer.default_collection:
                        continue
                    if template_defns is not None:
                        workload = template_defns.get(c)
                        if workload is None:
                            self.log.warning(f"{b.name}.{s}.{c} is not in the "
                                             f"workload template; no indexes "
                                             f"for it")
                            continue
                    else:
                        workloads = b.loadDefn.get("collections_defn", [b.loadDefn])
                        coll_order = TestInputSingleton.input.param("coll_order", 1)
                        if coll_order == -1:
                            workloads = list(reversed(workloads))
                        workload = workloads[collection_num % len(workloads)]
                    bucket_id = n1ql_identifier(b.name)
                    scope_id = n1ql_identifier(s)
                    coll_id = n1ql_identifier(c)
                    valType = workload.get("valType") or b.loadDefn.get("valType")
                    indexType = workload.get("indexes")
                    queryType = workload.get("queries")
                    queryParams = workload.get("queryParams")
                    if combinational:
                        indexType = workload.get("mix_indexes") or workload.get("indexes")
                        queryType = workload.get("mix_queries") or workload.get("queries")
                    elif base64:
                        indexType = workload.get("indexes_base64")
                        queryType = workload.get("queries_base64")
                    elif TestInputSingleton.input.param("bhive", False):
                        indexType = workload.get("bhive_indexes")
                        queryType = workload.get("queries")
                    if xattr:
                        indexType = [index.replace("`embedding`", "meta().xattrs.embedding") for index in indexType]
                        queryType = [query.replace("embedding", "meta().xattrs.embedding") for query in queryType]
                    if b.loadDefn.get("valType") == "siftBigANN":
                        queryParams = [{}] * workload.get("2i").get("num_queries")
                    i = 0
                    q = 0
                    while i < workload.get("2i").get("num_indexes") or q < workload.get("2i").get("num_queries"):
                        if i < workload.get("2i").get("num_indexes"):
                            vector_defn = {}
                            dim = None
                            idx_name = b.name.replace("-", "_") + "_idx_" + c + "_" + str(i)
                            if workload.get("vector"):
                                vector_defn = workload.get("vector")[vector_defn_counter % len(workload.get("vector"))]
                                dim = workload.get("dim")
                                if template_defns is not None \
                                        and vector_defn.get("similarity"):
                                    # A template prescribes the similarity
                                    similarity = vector_defn["similarity"]
                                else:
                                    similarity = TestInputSingleton.input.param(
                                        "similarity",
                                        random.choice(["L2_SQUARED", "EUCLIDEAN_SQUARED", "COSINE", "DOT"]))
                                nProbe = vector_defn["nProbe"]
                                description = vector_defn.get("description", "IVF,%s" % vector_defn.get("quantization", "SQ8"))
                                vector_fields = "'dimension': {}, 'description': '{}', 'similarity': '{}', 'scan_nprobes': {}".format(
                                dim, description, similarity, nProbe)
                                partitions = TestInputSingleton.input.param("partitions", None)
                                self.idx_q = indexType[i % len(indexType)]
                                if partitions:
                                    vector_fields = "'num_partition': {}, ".format(partitions) + vector_fields
                                    self.idx_q = indexType[i % len(indexType)].format(
                                        index_name=idx_name, bucket=bucket_id, scope=scope_id, collection=coll_id, vector=vector_fields)
                                else:
                                    self.idx_q = indexType[i % len(indexType)].format(
                                        index_name=idx_name, bucket=bucket_id, scope=scope_id, collection=coll_id, vector=vector_fields)
                                    self.idx_q = self.idx_q.replace('PARTITION BY HASH(meta().id)', "")
                            else:
                                self.idx_q = indexType[i % len(indexType)].format(index_name=idx_name, bucket=bucket_id, scope=scope_id, collection=coll_id)
                            b.indexes.update({idx_name: (self.idx_q, self.sdkClients[b.name+s], b.name, s, c)})
                            b.scopes[s].collections[c].secondary_indexes[idx_name] = self.idx_q
                            retry = 5
                            if not skip_index:
                                while retry > 0:
                                    try:
                                        execute_statement_on_n1ql(self.sdkClients[b.name+s], self.idx_q)
                                        break
                                    except QueryIndexAlreadyExistsException:
                                        break
                                    except Exception as e:
                                        import traceback
                                        traceback.print_exc()
                                        retry -= 1
                                        print(e)
                            i += 1
                            vector_defn_counter += 1
                        if q < workload.get("2i").get("num_queries"):
                            gt = None
                            unformatted_q = queryType[q % len(queryType)]
                            if workload.get("vector"):
                                gt = workload.get("groundTruths")
                                query = unformatted_q.format(bucket=bucket_id, scope=scope_id, collection=coll_id,
                                                             similarity=similarity, nProbe=nProbe)
                            else:
                                query = unformatted_q.format(bucket=bucket_id, scope=scope_id, collection=coll_id)
                            print (query)
                            if query not in b.query_map.keys():
                                b.query_map[query] = {"identifier": "C%s_Q%s" % (collection_num, q)}
                                if queryParams:
                                    b.query_map[query].update({"queryParams": queryParams[q % len(queryParams)]})
                                b.query_map[query].update(
                                    {
                                        "sdk":self.sdkClients[b.name+s],
                                        "vector_defn": vector_defn,
                                        "dim": dim,
                                        "collection": c
                                        }
                                    )
                                if gt:
                                    b.query_map[query].update({"gt": gt[q]})
                                if workload.get("es") and q < len(workload.get("es")):
                                    b.query_map[query].update({"es": workload.get("es")[q]})
                            q += 1

            query_tbl = TableView(self.log.info)
            query_tbl.set_headers(["Bucket", "##", "Query"])
            for k, v in sorted(b.query_map.items(), key=lambda x:x[1].get("identifier")):
                query_tbl.add_row([b.name, v["identifier"], k])
            query_tbl.display("N1QL Queries to run during test:")
        return True

    def discharge_N1QL(self):
        self.stop_run = True

    def query_result(self):
        return self.query_failure

    def build_indexes(self, cluster, buckets,
                      wait=False, timeout=86400):
        build = True
        build_count = TestInputSingleton.input.param("build_count", 5)
        # An indexer node builds at most 10 collections at once and fails the
        # 11th BUILD INDEX ("Build Already In Progress for 10 collections").
        # So each round is sent in batches of at most this many collections,
        # each waited for before the next; capping the whole batch keeps
        # every node within the limit.
        build_batch_size = TestInputSingleton.input.param("build_batch_size", 10)
        i = 0
        results = dict()
        while build:
            build = False
            k = 0
            while buckets[k*10:(k+1)*10]:
                _buckets = buckets[k*10:(k+1)*10]
                # (bucket, scope, collection, index names) for this round
                round_builds = []
                for bucket in _buckets:
                    d = defaultdict(list)
                    for key, val in bucket.indexes.items():
                        _, _, _, s, c = val
                        d[c].append(key)
                    for collection in sorted(d.keys())[i:i+build_count]:
                        round_builds.append((bucket, s, collection, d.get(collection)))
                for b in range(0, len(round_builds), build_batch_size):
                    batch = round_builds[b:b+build_batch_size]
                    for bucket, s, collection, index_names in batch:
                        details = bucket.indexes[index_names[0]]
                        build = True
                        build_query = "BUILD INDEX on %s.%s.%s(%s) USING GSI" % (
                            n1ql_identifier(bucket.name), n1ql_identifier(s),
                            n1ql_identifier(collection), ",".join(sorted(index_names)))
                        # time.sleep(1)
                        start = time.time()
                        while time.time() < start + 600:
                            try:
                                execute_statement_on_n1ql(details[1], build_query)
                                break
                            except Exception or InternalServerFailureException as e:
                                raise(e)
                    if wait:
                        self.rest = GsiHelper(cluster.master, logger["test"])
                        ths = list()

                        def polling_with_result(bucket, index_name, timeout_val):
                            result = self.rest.polling_create_index_status(bucket, index_name, timeout_val)
                            results[index_name] = result
                            return result

                        for bucket, _, _, index_names in batch:
                            for index_name in sorted(index_names):
                                th = threading.Thread(target=polling_with_result,
                                                      name=index_name,
                                                      args=(bucket, index_name, timeout//10))
                                th.start()
                                ths.append(th)
                        for th in ths:
                            th.join()

                        # Log the results
                        for index_name, result in results.items():
                            if result:
                                self.log.info("Index {} build completed successfully".format(index_name))
                            else:
                                self.log.error("Index {} build failed or timed out or warming up".format(index_name))
                k += 1
            i += build_count
        return results

    def drop_indexes(self, cluster):
        for bucket in cluster.buckets:
            for index, details in bucket.indexes.items():
                build_query = "DROP INDEX %s on `%s`" % (index, details[4])
                execute_statement_on_n1ql(details[1], build_query)

    def log_index_stats(self, cluster, print_duration=300):
        st_time = time.time()
        while not self.stop_run:
            self.table = TableView(self.log.info)
            self.table.set_headers(["Node",
                                    "mem_quota",
                                    "mem_used",
                                    "avg_rr",
                                    "avg_dr",
                                    "#data_size",
                                    "#disk_size",
                                    "#requests",
                                    "#rows_scanned",
                                    "#rows_returned"
                                    ])
            for node in cluster.index_nodes:
                try:
                    rest = RestConnection(node)
                    resp = rest.urllib_request(rest.indexUrl + "stats")
                    content = json.loads(resp.content)
                    self.table.add_row([
                        node.ip,
                        content["memory_quota"]/1024/1024/1024,
                        content["memory_used"]/1024/1024/1024,
                        content["avg_resident_percent"],
                        content["avg_drain_rate"],
                        content["total_data_size"]/1024/1024/1024,
                        content["total_disk_size"]/1024/1024/1024,
                        content["total_requests"],
                        content["total_rows_scanned"],
                        content["total_rows_returned"]
                        ])
                except Exception as e:
                    self.log.critical(e)

            if st_time + print_duration < time.time():
                self.table.display("Index Statistics")
                st_time = time.time()
            time.sleep(300)

    def start_index_stats(self, cluster):
        self.stop_run = False
        th = threading.Thread(target=self.log_index_stats,
                              kwargs=dict({"cluster": cluster}))
        th.start()

    def update_index_stats(self, cluster, interval=1200):
        for bucket in cluster.buckets:
            for idx_name, idx_details in bucket.indexes.items():
                update_q = "UPDATE STATISTICS FOR INDEX {3} on {0}.{1}.{2}".format(
                    n1ql_identifier(idx_details[2]), n1ql_identifier(idx_details[3]),
                    n1ql_identifier(idx_details[4]), idx_name)
                try:
                    execute_statement_on_n1ql(self.sdkClients[idx_details[2]+idx_details[3]], update_q)
                except CouchbaseException as e:
                    self.log.error(e)
            time.sleep(interval)

    def start_update_stats(self, cluster):
        self.stop_run = False
        th = threading.Thread(target=self.update_index_stats,
                              kwargs=dict({"cluster": cluster}))
        th.start()

class QueryLoad:
    groundTruths = {}
    queryVectors = []

    def __init__(self, mock_vector, bucket, validate_item_count=False, esClient=None, log_fail=True):
        self.log = logger.get("infra")
        self.valType = bucket.loadDefn.get("valType")
        self.base64 = False
        self.validate_item_count = validate_item_count
        # MSMARCO datasets have no ground truth, so there is no meaningful
        # recall/accuracy or result-count expectation. Disable result-count
        # validation for them (recall/accuracy compare is siftBigANN-only).
        if self.valType in ("MSMARCOEmbeddingProduct",
                            "MSMARCOSiftEmbeddingProduct"):
            self.validate_item_count = False
        self.bucket = bucket
        self.queries = [item[0] for item in sorted(bucket.query_map.items(), key=lambda x:x[1]["identifier"])]
        self.queries_meta = [item[1] for item in sorted(bucket.query_map.items(), key=lambda x:x[1]["identifier"])]
        if mock_vector:
            self.float_buf = [0] * 1024*1024
            for index in range(1024*1024):
                self.float_buf[index] = random.random()
            self.float_buf_length = len(self.float_buf)
        if self.valType == "siftBigANN":
            for meta in self.queries_meta:
                # A workload template collection may come without ground
                # truth; its queries then skip the recall/accuracy check
                if "gt" not in meta:
                    continue
                gt = meta["gt"][0]
                if gt not in QueryLoad.groundTruths.keys():
                    self.log.info("Reading gt file in memory: %s" % gt)
                    QueryLoad.groundTruths.update({gt: self.read_gt_vecs(os.path.join(self._sift_base_path(), gt))})
        # Per query: [total exec ms, count, accuracy, recall, ground-truth
        # label, lock, ES exec ms, ES recall, latest exec ms]. Built once for
        # the whole run, like the counters below. start_query_load() doesn't
        # reset them, so the per-query counts stay in step with the totals.
        sift = self.valType == "siftBigANN"
        self.query_stats = {query: [0, 0, 0, 0, self._gt_label(idx) if sift else "None", Lock(), 0, 0, []]
                            for idx, query in enumerate(self.queries)}
        self.failed_count = itertools.count()
        self.success_count = itertools.count()
        self.rejected_count = itertools.count()
        self.error_count = itertools.count()
        self.cancel_count = itertools.count()
        self.timeout_count = itertools.count()
        self.total_query_count = itertools.count()
        self.stop_run = False
        self.cluster_conn = self.queries_meta[0]["sdk"]
        self.concurrent_queries_to_run = self.bucket.loadDefn.get("2iQPS")
        self.failures = 0
        self.timeout_failures = 0
        self.esClient = esClient
        self.log_fail = log_fail

    def _sift_base_path(self):
        """
        Directory holding the siftBigANN query and ground-truth files. A
        workload template bucket names its own; the val_type workload's is
        the siftBigANN workload dict's.
        """
        if self.bucket.loadDefn.get(TEMPLATE_KEY):
            return self.bucket.loadDefn.get("baseFilePath")
        return siftBigANN.get("baseFilePath")

    def _gt_label(self, idx):
        meta = self.queries_meta[idx]
        return meta["gt"][1] if "gt" in meta else "None"

    def start_query_load(self):
        # Starts the worker threads only. A restart (restart_query_load,
        # resume after a pause) keeps the run's stats from __init__
        self.stop_run = False
        self.concurrent_queries_to_run = self.bucket.loadDefn.get("2iQPS")
        th = threading.Thread(target=self._run_concurrent_queries)
        th.start()

    def stop_query_load(self):
        self.stop_run = True

    def _run_concurrent_queries(self):
        threads = []
        method = self._run_query
        count = 100
        if self.valType == "siftBigANN":
            method = self._run_vector_query
            if not QueryLoad.queryVectors:
                QueryLoad.queryVectors = self.read_query_vecs(self._sift_base_path() + "/bigann_query.bvecs")
                count = 1000
        for i in range(0, self.concurrent_queries_to_run):
            threads.append(Thread(
                target=method,
                name="query_thread_{0}".format(self.bucket.name + str(i)),
                args=(self.validate_item_count, count)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    def _run_vector_query(self, validate_item_count=True, expected_count=100):
        name = threading.currentThread().getName()
        counter = 0
        while not self.stop_run:
            client_context_id = name + str(counter)
            start = time.time()
            e = ""
            try:
                next(self.total_query_count)
                query = self.queries[counter%len(self.queries)]
                query_tuple = self.queries_meta[counter%len(self.queries)]
                if self.esClient:
                    es_scalars = query_tuple["es"]
                    es_params = []
                    for k, v in es_scalars.items():
                        scalar = dict()
                        scalar[k] = v
                        term = dict()
                        term["term"] = scalar
                        es_params.append(term)
                vector_float = []
                index = self.query_stats[query][1] % len(QueryLoad.queryVectors)
                expected_count = 10
                embedding = QueryLoad.queryVectors[index]
                # None for a query without ground truth: it still runs, with
                # no recall/accuracy check
                groudtruth = None
                if "gt" in query_tuple:
                    gt = query_tuple["gt"][0]
                    groudtruth = QueryLoad.groundTruths[gt][index][:expected_count]

                for value in embedding:
                    vector_float.append(float(value))

                q_param = query_tuple["queryParams"]
                q_param.update({"vector": vector_float})
                status, metrics, _, results, _ = execute_statement_on_n1ql(
                    self.cluster_conn, query, client_context_id,
                    q_param, validate=validate_item_count)
                self.query_stats[query][0] += metrics.execution_time().total_seconds()*1000.0
                self.query_stats[query][1] += 1
                with self.query_stats[query][5]:
                    self.query_stats[query][8].append(metrics.execution_time().total_seconds()*1000.0)
                    if len(self.query_stats[query][8]) > 1000:
                        self.query_stats[query][8].pop(0)
                if status == QueryStatus.SUCCESS:
                    if self.esClient and groudtruth is not None:
                        recall_es, latency_es = performKNNSearchES(self.esClient, query_tuple["collection"], vector_float, 100, groudtruth, es_params)
                        self.query_stats[query][6] += latency_es
                        self.query_stats[query][7] += recall_es
                    if self.bucket.loadDefn.get("valType") == "siftBigANN" and validate_item_count:
                        results = [result.get("id") for result in list(results)]
                        if len(results) != expected_count:
                            self.log.critical("Expected Count:{}, Actual Count:{}".format(expected_count, len(results)))
                            next(self.failed_count)
                        else:
                            next(self.success_count)
                            if groudtruth is not None:
                                accuracy, recall = self.compare_result(results, groudtruth)
                                self.query_stats[query][2] += accuracy
                                self.query_stats[query][3] += round(100.0 * recall//len(results), 2)
                    else:
                        next(self.success_count)
                else:
                    next(self.failed_count)
            except CouchbaseException as ex:
                e = ex
            if str(e).find("TimeoutException") != -1\
                or str(e).find("AmbiguousTimeoutException") != -1\
                    or str(e).find("UnambiguousTimeoutException") != -1:
                self.timeout_failures += next(self.timeout_count)
                if self.timeout_failures % 50 == 0 or str(e).find("UnambiguousTimeoutException") != -1:
                    self.log.critical(client_context_id + ":" + query)
                    self.log.critical(e)
            elif str(e).find("RequestCanceledException") != -1:
                self.failures += next(self.cancel_count)
            elif str(e).find("InternalServerFailureException") != -1 or str(e).find("CouchbaseException") != -1:
                self.failures += next(self.error_count)

            if self.log_fail and e and (str(e).find("no more information available") != -1 or str(e).find("Index not ready for serving queries") != -1):
                self.log.critical(client_context_id + ":" + query)
                self.log.critical(e)
            end = time.time()
            counter += 1

    def _get_sparse_query_vector(self, nnz=None, vocab=None):
        # SPARSE_VECTOR_DISTANCE expects the query vector as a sparse vector of
        # the same shape as the stored `sparse` field: [[indices], [values]]
        # (ints and floats). Without ground-truth query files we can't build a
        # semantically meaningful vector, so generate one of the right FORMAT so
        # the query executes (recall is not meaningful for these mock queries).
        nnz = nnz or TestInputSingleton.input.param("sparse_nnz", 32)
        vocab = vocab or TestInputSingleton.input.param("sparse_vocab", 30522)
        indices = sorted(random.sample(range(vocab), min(nnz, vocab)))
        values = [random.random() for _ in indices]
        return [indices, values]

    def _run_query(self, validate_item_count=False, expected_count=0):
        name = threading.currentThread().getName()
        counter = 0
        while not self.stop_run:
            client_context_id = name + "__" + str(counter)
            counter += 1
            e = ""
            try:
                next(self.total_query_count)
                query = self.queries[counter%len(self.queries)]
                query_tuple = self.queries_meta[counter%len(self.queries)]
                q_param = query_tuple["queryParams"]
                if "SPARSE_VECTOR_DISTANCE" in query:
                    # Sparse vector search needs $vector as [[indices],[values]],
                    # not a dense float array.
                    q_param["vector"] = self._get_sparse_query_vector()
                elif query_tuple.get("dim") and "$vector" in query:
                    # Only a query that references $vector gets one. The
                    # scalar queries of a vector collection carry its dim
                    # too, and would otherwise need mockVector=True.
                    index = random.randint(0, self.float_buf_length-query_tuple.get("dim"))
                    q_param["vector"] = self.float_buf[index:index+query_tuple.get("dim")]
                status, metrics, _, results, _ = execute_statement_on_n1ql(
                    self.cluster_conn, query, client_context_id,
                    q_param, validate=validate_item_count)
                exec_time = metrics.execution_time().total_seconds()*1000.0

                self.query_stats[query][0] += exec_time
                self.query_stats[query][1] += 1
                self.query_stats[query][8].append(exec_time)
                if len(self.query_stats[query][8]) > 1000:
                    self.query_stats[query][8].pop(0)
                if status == QueryStatus.SUCCESS:
                    if validate_item_count:
                        if len(results) != expected_count:
                            next(self.failed_count)
                            # print ("Query Success but expected count mismatch.\n"
                            #        "Query: {}\n"
                            #        "Params: {}\n"
                            #        "Expected: {}\n"
                            #        "Actual: {}".format(query, q_
                            #                            param, expected_count, len(results)))
                        else:
                            next(self.success_count)
                    else:
                        next(self.success_count)
                else:
                    next(self.failed_count)
            except CouchbaseException as ex:
                e = ex
            if str(e).find("TimeoutException") != -1\
                or str(e).find("AmbiguousTimeoutException") != -1\
                    or str(e).find("UnambiguousTimeoutException") != -1:
                self.timeout_failures += next(self.timeout_count)
                self.log.critical(client_context_id + ":" + query)
                self.log.critical(e)
                if self.timeout_failures % 50 == 0 or str(e).find("UnambiguousTimeoutException") != -1:
                    self.log.critical(client_context_id + ":" + query)
                    self.log.critical(e)
            elif str(e).find("RequestCanceledException") != -1:
                self.failures += next(self.cancel_count)
            elif str(e).find("InternalServerFailureException") != -1 or str(e).find("CouchbaseException") != -1:
                self.failures += next(self.error_count)

            if e and (str(e).find("AmbiguousTimeoutException") == -1 or str(e).find("no more information available") != -1):
                self.log.critical(client_context_id + ":" + query)
                self.log.critical(e)
            end = time.time()

    def read_query_vecs(self, filename):
        '''
            4+d bytes for .bvecs
            4+d*4 bytes for .fvecs and .ivecs
            File: bigann_query.bvecs
        '''
        groundTruths = []
        # Read-only on purpose: the dataset usually sits on an NFS share
        # that squashes root, which refuses read-write ('+rb') opens
        f = open(filename, 'rb')

        for _ in range(10000):
            # Read the dimension (4 bytes)
            d_bytes = f.read(4)
            dim = struct.unpack('<i', d_bytes)[0]
            vector = f.read(dim)
            components = struct.unpack('<'+('B' * dim), vector)
            components = list(map(float, components))
            groundTruths.append(components)

        return groundTruths

    def read_gt_vecs(self, filename):
        '''
            4+d bytes for .bvecs
            4+d*4 bytes for .fvecs and .ivecs
            File: idx_10M.ivecs
        '''
        gtVectors = []
        # Read-only, as in read_query_vecs()
        f = open(filename, 'rb')

        for _ in range(10000):
            # Read the dimension (4 bytes)
            d_bytes = f.read(4)
            dim = struct.unpack('<i', d_bytes)[0]
            vector = f.read(dim*4)
            components = struct.unpack('<'+'i' * dim, vector)
            gtVectors.append(components)

        return gtVectors

    def compare_result(self, actual, expected):
        accuracy = 100 if actual and actual[0] == expected[0] else 0
        recall = 0
        for item in actual:
            if item in expected:
                recall += 1
        return accuracy, recall
