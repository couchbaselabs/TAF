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
from com.couchbase.client.java.analytics import AnalyticsOptions,\
    AnalyticsScanConsistency, AnalyticsStatus
from com.couchbase.client.core.error import RequestCanceledException,\
    CouchbaseException, AmbiguousTimeoutException
import traceback
from global_vars import logger
from Cb_constants.CBServer import CbServer
from hostedN1QL import execute_statement_on_n1ql

queries = ['select name from {} where age between 30 and 50 limit 10;',
           'select age, count(*) from {} where marital = "M" group by age order by age limit 10;',
           'select v.name, animal from {} as v unnest v.animals as animal where v.attributes.hair = "Burgundy" limit 10;',
           'select name, ROUND(attributes.dimensions.weight / attributes.dimensions.height,2) from {} WHERE gender is not MISSING limit 10;']
datasets = ['create dataset ds{} on {}.{}.{};']
HotelQueries = ["SELECT * from {} where phone like \"4%\" limit 100",
                "SELECT * from {} where `type` is not null limit 100",
                "SELECT COUNT(*) FILTER (WHERE free_breakfast = TRUE) AS count_free_breakfast, COUNT(*) FILTER (WHERE free_parking = TRUE) AS count_free_parking, COUNT(*) FILTER (WHERE free_breakfast = TRUE AND free_parking = TRUE) AS count_free_parking_and_breakfast FROM {} WHERE city LIKE 'North%' ORDER BY count_free_parking_and_breakfast DESC  limit 100",
                "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM {0} WHERE price IS NOT NULL GROUP BY city) SELECT h.name, h.price FROM {0} h JOIN city_avg ON h.city = city_avg.city WHERE h.price < city_avg.avgprice AND h.price IS NOT NULL limit 100",
                "select city,country,count(*) from {} where free_breakfast=True and free_parking=True group by country,city order by country,city limit 100",
                "select avg(price) as AvgPrice, min(price) as MinPrice, max(price) as MaxPrice from {} where free_breakfast=True and free_parking=True and price is not null and array_count(public_likes)>5 and `type`='Hotel' group by country limit 100",
                ]
HotelIndexes = [
"create index {0} on {1}(phone:string)",
"create index {0} on {1}(`type`:string)",
"create index {0} on {1}(city:string, country:string)",
"create index {0} on {1}(price:double)",
"create index {0} on {1}(avg_rating:double)",

]


class DoctorCBAS():

    def __init__(self, cluster, bucket_util,
                 num_idx=10, server_port=8095,
                 querycount=100, batch_size=50,
                 hotelquery=False):
        self.port = server_port
        self.failed_count = 0
        self.success_count = 0
        self.rejected_count = 0
        self.error_count = 0
        self.cancel_count = 0
        self.timeout_count = 0
        self.total_query_count = 0
        self.concurrent_batch_size = batch_size
        self.total_count = querycount
        self.num_datasets = num_idx
        self.bucket_util = bucket_util
        self.cluster = cluster
        self.log = logger.get("test")

        self.sdkClient = SDKClient(cluster.cbas_nodes, None)
        self.cluster_conn = self.sdkClient.cluster
        self.stop_run = False
        self.queries = list()
        self.datasets = dict()
        i = 0
        while i < self.num_datasets:
            for b in self.cluster.buckets:
                for s in self.bucket_util.get_active_scopes(b, only_names=True):
                    for c in sorted(self.bucket_util.get_active_collections(b, s, only_names=True)):
                        if c == CbServer.default_collection:
                            continue
                        self.idx_q = datasets[0].format(i, b.name, s, c)
                        self.datasets.update({"ds"+str(i): (self.idx_q, b.name, s, c)})
                        if hotelquery:
                            self.queries.append(HotelQueries[i % len(HotelQueries)].format("ds" + str(i)))
                        else:
                            self.queries.append(queries[i % len(queries)].format("ds"+str(i)))
                        i += 1
                        if i >= self.num_datasets:
                            break
                    if i >= self.num_datasets:
                        break
                if i >= self.num_datasets:
                    break

    def discharge_CBAS(self):
        self.stop_run = True

    def create_datasets(self):
        for index in self.datasets.values():
            time.sleep(1)
            self.execute_statement_on_cbas(index[0])

    def create_indexes(self, hotel_index=True):
        i=0
        for dataset in self.datasets.keys():
            if hotel_index:
                idx_query = HotelIndexes[i % len(HotelIndexes)].format("idx" + dataset, dataset)
            self.execute_statement_on_cbas(idx_query)
            i+=1

    def wait_for_ingestion(self, item_count, timeout=86400):
        status = False
        for dataset in self.datasets.keys():
            status = False
            stop_time = time.time() + timeout
            while time.time() < stop_time:
                statement = "select count(*) count from {};".format(dataset)
                _status, _, _, results, _ = self.execute_statement_on_cbas(statement)
                self.log.debug("dataset: {}, status: {}, count: {}"
                               .format(dataset, _status,
                                       json.loads(str(results))[0]["count"]))
                if json.loads(str(results))[0]["count"] == item_count:
                    self.log.info("CBAS dataset is ready: {}".format(dataset))
                    status = True
                    break
                time.sleep(5)
            if status is False:
                return status
        return status

    def wait_for_ingestion_on_all_datasets(self, cluster, timeout=86400):
        n1ql_sdk_client = SDKClient(cluster.query_nodes, None)
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
                _status, _, _, results, _ = self.execute_statement_on_cbas(statement)
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

    def start_query_load(self):
        th = threading.Thread(target=self._run_concurrent_queries,
                              kwargs=dict(num_queries=self.num_datasets))
        th.start()

        monitor = threading.Thread(target=self.monitor_query_status,
                                   kwargs=dict(duration=0,
                                               print_duration=60))
        monitor.start()

    def _run_concurrent_queries(self, num_queries):
        threads = []
        self.total_query_count = 0
        for _ in range(0, num_queries):
            self.total_query_count += 1
            threads.append(Thread(
                target=self._run_query,
                name="query_thread_{0}".format(self.total_query_count),
                args=(random.choice(self.queries), False, 0)))

        i = 0
        for thread in threads:
            i += 1
            if i % self.concurrent_batch_size == 0:
                time.sleep(5)
            thread.start()

        i = 0
        while not self.stop_run:
            threads = []
            new_queries_to_run = num_queries - self.total_count
            for i in range(0, new_queries_to_run):
                self.total_query_count += 1
                threads.append(Thread(
                    target=self._run_query,
                    name="query_thread_{0}".format(self.total_query_count),
                    args=(random.choice(self.queries), False, 0)))
            i = 0
            self.total_count += new_queries_to_run
            for thread in threads:
                i += 1
                if i % self.concurrent_batch_size == 0:
                    time.sleep(5)
                thread.start()

            time.sleep(2)
        if self.failed_count + self.error_count != 0:
            raise Exception("Queries Failed:%s , Queries Error Out:%s" %
                            (self.failed_count, self.error_count))

    def _run_query(self, query, validate_item_count=False, expected_count=0):
        name = threading.currentThread().getName()
        client_context_id = name
        try:
            status, _, _, results, _ = self.execute_statement_on_cbas(
                query, client_context_id=client_context_id)
            if status == AnalyticsStatus.SUCCESS:
                if validate_item_count:
                    if results[0]['$1'] != expected_count:
                        self.failed_count += 1
                        self.total_count -= 1
                    else:
                        self.success_count += 1
                        self.total_count -= 1
                else:
                    self.success_count += 1
                    self.total_count -= 1
            else:
                self.failed_count += 1
                self.total_count -= 1
        except Exception as e:
            if e == AmbiguousTimeoutException:
                self.timeout_count += 1
                self.total_count -= 1
            elif e == RequestCanceledException:
                self.cancel_count += 1
                self.total_count -= 1
            elif e == CouchbaseException:
                self.rejected_count += 1
                self.total_count -= 1
            else:
                print(e)
                traceback.print_exc()
                self.error_count += 1
                self.total_count -= 1

    def execute_statement_on_cbas(self, statement,
                                  client_context_id=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        try:
            response = self.execute_via_sdk(
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

    def execute_via_sdk(self, statement, readonly=False,
                        client_context_id=None):
        options = AnalyticsOptions.analyticsOptions()
        options.scanConsistency(AnalyticsScanConsistency.NOT_BOUNDED)
        options.readonly(readonly)
        if client_context_id:
            options.clientContextId(client_context_id)

        output = {}
        try:
            result = self.cluster_conn.analyticsQuery(statement)

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

        except AmbiguousTimeoutException as e:
            raise Exception(e)
        except RequestCanceledException as e:
            raise Exception(e)
        except CouchbaseException as e:
            raise Exception(e)
        except Exception as e:
            print(e)
            traceback.print_exc()
        return output

    def monitor_query_status(self, duration=0, print_duration=600):
        st_time = time.time()
        update_time = time.time()
        if duration == 0:
            while True:
                if st_time + print_duration < time.time():
                    print("%s CBAS queries submitted, %s failed, \
                        %s passed, %s rejected, \
                        %s cancelled, %s timeout %s errored" % (
                        self.total_query_count, self.failed_count,
                        self.success_count, self.rejected_count,
                        self.cancel_count, self.timeout_count,
                        self.error_count))
                    st_time = time.time()
        else:
            while st_time + duration > time.time():
                if update_time + print_duration < time.time():
                    print("%s CBAS queries submitted, %s failed, \
                        %s passed, %s rejected, \
                        %s cancelled, %s timeout %s errored" % (
                        self.total_query_count, self.failed_count,
                        self.success_count, self.rejected_count,
                        self.cancel_count, self.timeout_count,
                        self.error_count))
                    update_time = time.time()
