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
from com.couchbase.client.core.deps.io.netty.handler.timeout import TimeoutException
from com.couchbase.client.core.error import RequestCanceledException,\
    CouchbaseException, AmbiguousTimeoutException
import traceback
from global_vars import logger
from Cb_constants.CBServer import CbServer

queries = ['select name from {} where age between 30 and 50 limit 10;',
           'select age, count(*) from {} where marital = "M" group by age order by age limit 10;',
           'select v.name, animal from {} as v unnest v.animals as animal where v.attributes.hair = "Burgundy" limit 10;',
           'select name, ROUND(attributes.dimensions.weight / attributes.dimensions.height,2) from {} WHERE gender is not MISSING limit 10;']
datasets = ['create dataset ds{} on {}.{}.{};']


class DoctorCBAS():

    def __init__(self, cluster, bucket_util,
                 num_idx=10, server_port=8095,
                 querycount=100, batch_size=50):
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

    def wait_for_ingestion(self, item_count, timeout=86400):
        status = False
        for dataset in self.datasets.keys():
            status = False
            stop_time = time.time() + timeout
            while time.time() < stop_time:
                statement = "select count(*) count from {};".format(dataset)
                status, _, _, results, _ = self.execute_statement_on_cbas(statement)
                self.log.debug("dataset: {}, status: {}, count: {}"
                               .format(dataset, status,
                                       json.loads(str(results))[0]["count"]))
                if json.loads(str(results))[0]["count"] == item_count:
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
