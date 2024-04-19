'''
Created on 15-Apr-2021

@author: riteshagarwal
'''
import json
import random
from threading import Thread
import threading
import time

from sdk_client3 import SDKClient
from global_vars import logger
from constants.cb_constants.CBServer import CbServer
import itertools
from n1ql import execute_statement_on_n1ql

datasets = ['create dataset {} on {}.{}.{};']

queries = [
    'select name from {} where age between 30 and 50 limit 10;',
    'select age, count(*) from {} where marital = "M" group by age order by age limit 10;',
    'select v.name, animal from {} as v unnest v.animals as animal where v.attributes.hair = "Burgundy" limit 10;',
    'select name, ROUND(attributes.dimensions.weight / attributes.dimensions.height,2) from {} WHERE gender is not MISSING limit 10;']
indexes = [
    "create index {0} on {1}(age:BIGINT)",
    "create index {0} on {1}(marital:string, age:BIGINT)",
    "create index {0} on {1}(gender:string)"]

HotelQueries = [
    "SELECT * from {} where phone like \"4%\" limit 100",
    "SELECT * from {} where `type` is not null limit 100",
    # "SELECT COUNT(*) FILTER (WHERE free_breakfast = TRUE) AS count_free_breakfast, COUNT(*) FILTER (WHERE free_parking = TRUE) AS count_free_parking, COUNT(*) FILTER (WHERE free_breakfast = TRUE AND free_parking = TRUE) AS count_free_parking_and_breakfast FROM {} WHERE city LIKE 'North%' ORDER BY count_free_parking_and_breakfast DESC  limit 100",
    "SELECT COUNT(CASE WHEN free_breakfast THEN 1 ELSE NULL END) AS count_free_breakfast, COUNT(CASE WHEN free_parking THEN 1 ELSE NULL END) AS count_free_parking, COUNT(CASE WHEN free_parking AND free_breakfast THEN 1 ELSE NULL END) AS count_free_parking_and_breakfast FROM {} WHERE city LIKE 'North%' ORDER BY count_free_parking_and_breakfast DESC  limit 100",
    "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM {0} WHERE price IS NOT NULL GROUP BY city) SELECT h.name, h.price FROM {0} h JOIN city_avg ON h.city = city_avg.city WHERE h.price < city_avg.avgprice AND h.price IS NOT NULL limit 100",
    "select city,country,count(*) from {} where free_breakfast=True and free_parking=True group by country,city order by country,city limit 100",
    "select avg(price) as AvgPrice, min(price) as MinPrice, max(price) as MaxPrice from {} where free_breakfast=True and free_parking=True and price is not null and array_count(public_likes)>5 and `type`='Hotel' group by country limit 100",
    ]
HotelIndexes = [
    "create index {0} on {1}(phone:string)",
    "create index {0} on {1}(`type`:string)",
    "create index {0} on {1}(city:string, country:string)",
    "create index {0} on {1}(price:double)",
    "create index {0} on {1}(avg_rating:double)"]


def execute_statement_on_cbas(client, statement,
                              client_context_id=None):
    """
    Executes a statement on CBAS using the REST API using REST Client
    """
    try:
        response = execute_via_sdk(client,
                                   statement, False, client_context_id)

        if type(response) == str:
            response = json.loads(response)
        if "errors" in response:
            errors = response["errors"]
        else:
            errors = None

        if "results" in response:
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
        return response["status"], metrics, errors, results, handle

    except Exception as e:
        raise Exception(str(e))


def execute_via_sdk(client, statement, readonly=False,
                    client_context_id=None):
    options = AnalyticsOptions.analyticsOptions()
    options.scanConsistency(AnalyticsScanConsistency.NOT_BOUNDED)
    options.readonly(readonly)
    if client_context_id:
        options.clientContextId(client_context_id)

    output = {}
    result = client.analyticsQuery(statement, options)

    output["status"] = result.metaData().status()
    output["metrics"] = result.metaData().metrics()

    try:
        output["results"] = result.rowsAsObject()
    except:
        output["results"] = None

    if str(output['status']) == AnalyticsStatus.FATAL:
        msg = output['errors'][0]['msg']
        if "Job requirement" in msg and "exceeds capacity" in msg:
            raise Exception("Capacity cannot meet job requirement")
    elif output['status'] == AnalyticsStatus.SUCCESS:
        output["errors"] = None
    else:
        raise Exception("Analytics Service API failed")

    return output


class DoctorCBAS():

    def __init__(self, bucket_util):
        self.bucket_util = bucket_util
        self.log = logger.get("test")
        self.stop_run = False

    def create_datasets(self, buckets):
        for b in buckets:
            b.cbas_queries = list()
            b.datasets = dict()
            b.query_map = dict()
            i = 0
            query_count = 0
            for s in self.bucket_util.get_active_scopes(b, only_names=True):
                if s == CbServer.system_scope:
                    continue
                for collection_num, c in enumerate(sorted(self.bucket_util.get_active_collections(b, s, only_names=True))):
                    if c == CbServer.default_collection:
                        continue
                    workloads = b.loadDefn.get("collections_defn", [b.loadDefn])
                    workload = workloads[collection_num % len(workloads)]
                    valType = workload["valType"]
                    indexType = indexes
                    queryType = queries
                    if valType == "Hotel":
                        indexType = HotelIndexes
                        queryType = HotelQueries
                    d = 0
                    i = 0
                    q = 0
                    while i < workload.get("cbas")[0] or q < workload.get("cbas")[1]:
                        dataset = "{}_{}_ds{}".format(b.name, c, str(i))
                        if d < workload.get("cbas")[0]:
                            dataset_q = datasets[i % len(datasets)].format(dataset, b.name, s, c)
                            try:
                                execute_statement_on_cbas(b.clients[0].cluster, dataset_q)
                            except DatasetExistsException:
                                pass
                            d += 1
                        if i < workload.get("cbas")[1]:
                            idx_q = indexType[i % len(indexType)].format("idx_" + dataset, dataset)
                            b.datasets.update({dataset: idx_q})
                            i += 1
                        if q < workload.get("cbas")[2]:
                            if queryType[q % len(queryType)] in b.query_map.keys():
                                q += 1
                                continue
                            query = queryType[q % len(queryType)].format(dataset)
                            print query
                            b.query_map[queryType[q % len(queryType)]] = ["Q%s" % query_count]
                            query_count += 1
                            b.cbas_queries.append((query, queryType[q % len(queryType)]))
                            q += 1

    def discharge_CBAS(self):
        self.stop_run = True

    def create_indexes(self, buckets):
        for bucket in buckets:
            for index in bucket.datasets.values():
                try:
                    execute_statement_on_cbas(bucket.clients[0].cluster, index)
                except IndexExistsException:
                    pass

    def wait_for_ingestion(self, buckets, timeout=86400):
        status = True
        for bucket in buckets:
            for dataset in bucket.datasets.keys():
                status = False
                stop_time = time.time() + timeout
                while time.time() < stop_time:
                    statement = "select count(*) count from {};".format(dataset)
                    try:
                        _status, _, _, results, _ = execute_statement_on_cbas(bucket.clients[0].cluster, statement)
                        self.log.debug("dataset: {}, status: {}, actual count: {}, expected count: {}"
                                       .format(dataset, _status,
                                               json.loads(str(results))[0]["count"],
                                               bucket.loadDefn.get("num_items")))
                        if json.loads(str(results))[0]["count"] == bucket.loadDefn.get("num_items"):
                            self.log.info("CBAS dataset is ready: {}".format(dataset))
                            status = True
                            break
                    except:
                        pass
                    time.sleep(5)
                if status is False:
                    return status
        return status

    def wait_for_ingestion_on_all_datasets(self, cluster, timeout=86400):
        n1ql_sdk_client = SDKClient(cluster, None, servers=cluster.query_nodes)
        cluster_conn = n1ql_sdk_client.cluster
        n1ql_sdkClients = dict()
        for bucket in cluster.buckets:
            for s in self.bucket_util.get_active_scopes(bucket, only_names=True):
                if bucket.name + s not in n1ql_sdkClients.keys():
                    n1ql_sdkClients.update({bucket.name + s: cluster_conn.bucket(bucket.name).scope(s)})
                    time.sleep(5)

        status = False
        for dataset, ds_info in self.datasets.items():

            n1ql_statement = "select count(*) from {0}.{1}.{2}".format(
                ds_info[1], ds_info[2], ds_info[3])
            _status, _, _, n1ql_results, _ = execute_statement_on_n1ql(
                n1ql_sdkClients[ds_info[1] + ds_info[2]], n1ql_statement)
            self.log.debug("collection: {}.{}.{}, status: {}, count: {}"
                           .format(ds_info[1], ds_info[2], ds_info[3], _status,
                                   json.loads(str(n1ql_results))[0]["$1"]))

            status = False
            stop_time = time.time() + timeout
            while time.time() < stop_time:
                statement = "select count(*) count from {};".format(dataset)
                _status, _, _, results, _ = execute_statement_on_cbas(statement)
                self.log.debug("dataset: {}, status: {}, count: {}"
                               .format(dataset, _status,
                                       json.loads(str(results))[0]["count"]))

                if json.loads(str(results))[0]["count"] == json.loads(str(n1ql_results))[0]["$1"]:
                    self.log.info("CBAS dataset is ready: {}".format(dataset))
                    status = True
                    break
                time.sleep(5)
            if status is False:
                return status
        return status


class CBASQueryLoad:
    def __init__(self, bucket):
        self.bucket = bucket
        self.queries = bucket.cbas_queries
        self.failed_count = itertools.count()
        self.success_count = itertools.count()
        self.rejected_count = itertools.count()
        self.error_count = itertools.count()
        self.cancel_count = itertools.count()
        self.timeout_count = itertools.count()
        self.total_query_count = itertools.count()
        self.stop_run = False
        self.log = logger.get("infra")
        self.concurrent_queries_to_run = self.bucket.loadDefn.get("cbasQPS")
        self.query_stats = {key[1]: [0, 0] for key in self.queries}
        self.failures = 0
        self.cluster_conn = random.choice(self.bucket.clients).cluster

    def start_query_load(self):
        th = threading.Thread(target=self._run_concurrent_queries)
        th.start()

    def stop_query_load(self):
        self.stop_run = True

    def _run_concurrent_queries(self):
        threads = []
        for i in range(0, self.concurrent_queries_to_run):
            threads.append(Thread(
                target=self._run_query,
                name="query_thread_{0}".format(self.bucket.name + str(i)),
                args=(False, 0)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    def _run_query(self, validate_item_count=False, expected_count=0):
        name = threading.currentThread().getName()
        client_context_id = name
        while not self.stop_run:
            start = time.time()
            e = ""
            try:
                self.total_query_count.next()
                query_tuple = random.choice(self.queries)
                query = query_tuple[0]
                original_query = query_tuple[1]
                # print query
                # print original_query
                status, metrics, _, results, _ = execute_statement_on_cbas(
                    self.cluster_conn, query, client_context_id)
                self.query_stats[original_query][0] += metrics.executionTime().toNanos()/1000000.0
                self.query_stats[original_query][1] += 1
                if status == AnalyticsStatus.SUCCESS:
                    if validate_item_count:
                        if results[0]['$1'] != expected_count:
                            self.failed_count.next()
                        else:
                            self.success_count.next()
                    else:
                        self.success_count.next()
                else:
                    self.failed_count.next()
            except TimeoutException or AmbiguousTimeoutException or UnambiguousTimeoutException as e:
                pass
            except RequestCanceledException as e:
                pass
            except CouchbaseException as e:
                pass
            except (Exception, PlanningFailureException) as e:
                print e
                self.error_count.next()
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
                self.log.critical(client_context_id)
            end = time.time()
            if end - start < 1:
                time.sleep(end - start)
