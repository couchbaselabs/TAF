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
    CouchbaseException


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
        self.cluster = cluster

        self.sdkClient = SDKClient(self.cluster.cbas_nodes, None)
        self.cluster_conn = self.sdkClient.cluster
        self.stop_run = False
        self.queries = list()
        self.num_indexes = num_idx
        i = 0
        while i < self.num_indexes:
            for bucket in self.cluster.buckets:
                for scope in bucket_util.get_active_scopes(bucket, only_names=True):
                    for collection in sorted(bucket_util.get_active_collections(bucket, scope, only_names=True)):
                        self.initial_idx = "ds"
                        self.initial_idx_q = "CREATE DATASET %s%s on `%s`.`%s`.`%s`;" % (
                            self.initial_idx, i, bucket.name,
                            scope, collection)
                        self.execute_statement_on_cbas_util(self.initial_idx_q)
                        self.queries.append("select body from %s%s limit 1;" % (self.initial_idx, i))
                        i += 1
        th = threading.Thread(target=self._run_concurrent_queries,
                              kwargs=dict(num_queries=self.num_indexes))
        th.start()

    def discharge_CBAS(self):
        self.stop_run = True

    def _run_concurrent_queries(self, num_queries):
        threads = []
        total_query_count = 0
        query_count = 0
        for i in range(0, num_queries):
            total_query_count += 1
            threads.append(Thread(
                target=self._run_query,
                name="query_thread_{0}".format(total_query_count),
                args=(random.choice(self.queries), False, 0)))

        i = 0
        for thread in threads:
            i += 1
            if i % self.concurrent_batch_size == 0:
                time.sleep(5)
            thread.start()
            self.total_query_count += 1
            query_count += 1

        i = 0
        while not self.stop_run:
            threads = []
            new_queries_to_run = num_queries - self.total_count
            for i in range(0, new_queries_to_run):
                total_query_count += 1
                threads.append(Thread(
                    target=self._run_query,
                    name="query_thread_{0}".format(total_query_count),
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
            status, _, _, results, _ = self.execute_statement_on_cbas_util(
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
            if e == TimeoutException:
                self.timeout_count += 1
                self.total_count -= 1
            elif e == RequestCanceledException:
                self.cancel_count += 1
                self.total_count -= 1
            elif e == CouchbaseException:
                self.rejected_count += 1
                self.total_count -= 1
            else:
                self.error_count += 1
                self.total_count -= 1

    def execute_statement_on_cbas_util(self, statement,
                                       client_context_id=None):
        """
        Executes a statement on CBAS using the REST API using REST Client
        """
        try:
            response = self.execute_statement_on_cbas(
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

    def execute_statement_on_cbas(self, statement, readonly=False,
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

        except TimeoutException as e:
            raise Exception(e)
        except RequestCanceledException as e:
            raise Exception(e)
        except CouchbaseException as e:
            raise Exception(e)
        return output

    def monitor_query_status(self, duration=0, print_duration=600):
        st_time = time.time()
        update_time = time.time()
        if duration == 0:
            while True:
                if st_time + print_duration < time.time():
                    print("%s queries submitted, %s failed, \
                        %s passed, %s rejected, \
                        %s cancelled, %s timeout" % (
                        self.total_query_count, self.failed_count,
                        self.success_count, self.rejected_count,
                        self.cancel_count, self.timeout_count))
                    st_time = time.time()
        else:
            while st_time + duration > time.time():
                if update_time + print_duration < time.time():
                    print("%s queries submitted, %s failed, \
                        %s passed, %s rejected, \
                        %s cancelled, %s timeout" % (
                        self.total_query_count, self.failed_count,
                        self.success_count, self.rejected_count,
                        self.cancel_count, self.timeout_count))
                    update_time = time.time()
