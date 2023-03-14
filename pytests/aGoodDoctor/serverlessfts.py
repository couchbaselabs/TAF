'''
Created on May 31, 2022

@author: ritesh.agarwal
'''

import itertools
import json
import random
from threading import Thread
import threading
import time

from Cb_constants.CBServer import CbServer
from FtsLib.FtsOperations import FtsHelper
from TestInput import TestInputSingleton
from com.couchbase.client.core.deps.io.netty.handler.timeout import TimeoutException
from com.couchbase.client.core.error import AmbiguousTimeoutException, \
 RequestCanceledException, CouchbaseException, UnambiguousTimeoutException
from com.couchbase.client.java.search import SearchQuery
from global_vars import logger
from sdk_client3 import SDKClient
from table_view import TableView
from membase.api.rest_client import RestConnection
from _collections import defaultdict


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
        self.stop_run = False

    def monitor_fts_auto_scaling(self, dataplane_id):
        '''
        1. Monitor when the FTS scaling should trigger.
        2. Wait for FTS scaling to trigger
        3. Assert the number of FTS nodes in the cluster
        '''
        pass

    def create_fts_indexes(self, buckets):
        for b in buckets:
            i = 0
            b.ftsIndexes = dict()
            b.ftsQueries = dict()
            if b.loadDefn.get("FTS")[0] == 0:
                continue
            self.log.info("Creating FTS indexes on {}".format(b.name))
            self.fts_helper = FtsHelper(b.serverless.nebula_endpoint)
            for s in self.bucket_util.get_active_scopes(b, only_names=True):
                for c in sorted(self.bucket_util.get_active_collections(b, s, only_names=True)):
                    if c == CbServer.default_collection:
                        continue
                    fts_param_template = self.get_fts_idx_template()
                    fts_param_template.update({
                        "name": str(b.name.replace("-", "_")) + "_fts_idx_{}".format(i), "sourceName": str(b.name)})
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
                    self.log.debug("Creating fts index: {}".format(b.name.replace("-", "_") + "_fts_idx_"+str(i)))
                    retry = 10
                    while retry > 0:
                        status, _ = self.fts_helper.create_fts_index_from_json(
                            b.name.replace("-", "_")+"_fts_idx_"+str(i), str(fts_param_template))
                        if status is False:
                            self.log.critical("FTS index creation failed")
                            time.sleep(10)
                            retry -= 1
                        else:
                            b.ftsIndexes.update({str(b.name).replace("-", "_")+"_fts_idx_"+str(i): (fts_param_template, b.name, s, c)})
                            break
                    i += 1
                    time.sleep(10)
                    if i >= b.loadDefn.get("FTS")[0]:
                        break
                if i >= b.loadDefn.get("FTS")[0]:
                    break

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

    def wait_for_fts_index_online(self, buckets, timeout=86400,
                                  overRideCount=None):
        status = True
        for bucket in buckets:
            self.fts_helper = FtsHelper(bucket.serverless.nebula_endpoint)
            for index_name, details in bucket.ftsIndexes.items():
                status = False
                stop_time = time.time() + timeout
                while time.time() < stop_time:
                    try:
                        _status, content = self.fts_helper.fts_index_item_count(
                            "%s" % (index_name))
                        self.log.debug("index: {}, status: {}, count: {}"
                                       .format(index_name, _status,
                                               json.loads(content)["count"]))
                        if overRideCount is not None and overRideCount == json.loads(content)["count"] or\
                           json.loads(content)["count"] == bucket.loadDefn.get("num_items"):
                            self.log.info("FTS index is ready: {}".format(index_name))
                            status = True
                            break
                    except:
                        pass
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

    def index_stats(self, dataplanes):
        for dataplane in dataplanes.values():
            stat_monitor = threading.Thread(target=self.log_index_stats,
                                            kwargs=dict(dataplane=dataplane,
                                                        print_duration=60))
            stat_monitor.start()

    def log_index_stats(self, dataplane, print_duration=600):
        st_time = time.time()
        self.scale_down = False
        self.scale_up = False
        self.fts_auto_rebl = False
        mem_prof = True
        self.last_30_mins = defaultdict(list)
        while not self.stop_run:
            self.nodes_under_uwm = 0
            self.nodes_above_lwm = 0
            self.nodes_above_hwm = 0
            self.hwm_nodes_can_defragmented = 0
            collect_logs = False
            defrag_data = dict()
            self.table = TableView(self.log.info)
            self.table.set_headers(["Node",
                                    "memoryBytes",
                                    "diskBytes",
                                    "billableUnitsRate",
                                    "cpuPercent",
                                    "5 min Avg Mem",
                                    "5 min Avg CPU"])
            rest = RestConnection(dataplane.master)
            defrag = rest.urllib_request(rest.baseUrl + "pools/default/services/fts/defragmented")
            defrag = json.loads(defrag.content)
            self.defrag_table = TableView(self.log.info)
            self.defrag_table.set_headers(["Node",
                                           "memoryBytes",
                                           "diskBytes",
                                           "billableUnitsRate",
                                           "cpuPercent"
                                           ])
            for node, fts_stat in defrag.items():
                self.defrag_table.add_row([node,
                                           fts_stat["memoryBytes"],
                                           fts_stat["diskBytes"],
                                           fts_stat["billableUnitsRate"],
                                           fts_stat["cpuPercent"]
                                           ])
                defrag_data.update({node.split(":")[0]: (fts_stat["memoryBytes"],
                                                         fts_stat["diskBytes"],
                                                         fts_stat["billableUnitsRate"],
                                                         fts_stat["cpuPercent"])})
            for node in dataplane.fts_nodes:
                try:
                    rest = RestConnection(node)
                    content = rest.get_fts_stats()
                    mem_used = content["utilization:memoryBytes"]*1.0/content["limits:memoryBytes"]
                    cpu_used = content["utilization:cpuPercent"]
                    uwm = content["resourceUnderUtilizationWaterMark"]
                    lwm = content["resourceUtilizationLowWaterMark"]
                    hwm = content["resourceUtilizationHighWaterMark"]
                    if len(self.last_30_mins[node.ip]) > 30:
                        self.last_30_mins[node.ip].pop(0)
                    self.last_30_mins[node.ip].append((mem_used, cpu_used))
                    avg_mem_used = sum([consumption[0] for consumption in self.last_30_mins[node.ip]])/len(self.last_30_mins[node.ip])
                    avg_cpu_used = sum([consumption[1] for consumption in self.last_30_mins[node.ip]])/len(self.last_30_mins[node.ip])
                    if mem_used > 1.05 and mem_prof:
                        self.log.critical("This should trigger FTS memory profile capture")
                        FtsHelper(node).capture_memory_profile()
                        collect_logs = True
                        mem_prof = False
                    if avg_mem_used < uwm and avg_cpu_used < uwm*100:
                        self.nodes_under_uwm += 1
                    if avg_mem_used > hwm or avg_cpu_used > hwm*100:
                        self.nodes_above_hwm += 1
                        if defrag_data[node.ip][0]*1.0/content["limits:memoryBytes"] < hwm or\
                                defrag_data[node.ip][3]*1.0/100 < hwm:
                                self.hwm_nodes_can_defragmented += 1
                    if avg_mem_used > lwm or avg_cpu_used > lwm*100:
                        self.nodes_above_lwm += 1

                    self.table.add_row([
                        node.ip,
                        "{}/{}".format(str(content["utilization:memoryBytes"]/1024/1024),
                                       str(content["limits:memoryBytes"]/1024/1024)),
                        "{}/{}".format(str(content["utilization:diskBytes"]/1024/1024),
                                       str(content["limits:diskBytes"]/1024/1024)),
                        "{}/{}".format(str(content["utilization:billableUnitsRate"]),
                                       str(content["limits:billableUnitsRate"])),
                        "{}".format(str(content["utilization:cpuPercent"])),
                        avg_mem_used,
                        avg_cpu_used
                        ])
                except Exception as e:
                    self.log.critical(e)

            if st_time + print_duration < time.time() or self.scale_down and self.scale_up or self.fts_auto_rebl:
                self.log.info("FTS - Nodes below UWM: {}".format(self.nodes_under_uwm))
                self.log.info("FTS - Nodes above LWM: {}".format(self.nodes_above_lwm))
                self.log.info("FTS - Nodes above HWM: {}".format(self.nodes_above_hwm))
                self.log.info("FTS - Nodes at HWM and can be defragmented: {}".format(self.hwm_nodes_can_defragmented))
                self.table.display("FTS Statistics")
                self.defrag_table.display("FTS Defrag Stats")
                st_time = time.time()

            if collect_logs:
                self.log.critical("Please collect logs immediately!!!")
                pass

            if self.scale_down is False and self.scale_up is False and self.fts_auto_rebl is False:
                if self.nodes_under_uwm == len(dataplane.fts_nodes)\
                        and self.scale_down is False\
                        and len(dataplane.fts_nodes) > 2:
                    self.scale_down = True
                    self.log.info("FTS - Scale DOWN should trigger in a while")
                elif len(dataplane.fts_nodes) < 10\
                    and self.nodes_above_lwm == len(dataplane.fts_nodes)\
                        and self.scale_up is False:
                    self.scale_up = True
                    self.log.info("FTS - Scale UP should trigger in a while")
                elif self.nodes_above_hwm > 0 and self.hwm_nodes_can_defragmented > 0 and self.fts_auto_rebl is False:
                    self.fts_auto_rebl = True
                    self.log.info("FTS - Auto-Rebalance should trigger in a while")
            time.sleep(60)


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
        th = threading.Thread(target=self._run_concurrent_queries,
                              kwargs=dict(bucket=self.bucket))
        th.start()

    def stop_query_load(self):
        self.stop_run = True
        try:
            if self.cluster_conn:
                self.cluster_conn.close()
        except:
            pass

    def _run_concurrent_queries(self, bucket):
        threads = []
        self.total_query_count = 0
        self.concurrent_queries_to_run = bucket.loadDefn.get("FTS")[1]
        self.currently_running = 0
        query_count = 0
        for i in range(0, self.concurrent_queries_to_run):
            self.total_query_count += 1
            self.currently_running += 1
            query = random.choice(ftsQueries)
            index, details = random.choice(bucket.ftsIndexes.items())
            _, b, s, _ = details
            threads.append(Thread(
                target=self._run_query,
                name="query_thread_{0}".format(self.total_query_count),
                args=(index, query, b, s)))

        i = 0
        for thread in threads:
            i += 1
            thread.start()
            query_count += 1

        i = 0
        while not self.stop_run:
            threads = []
            new_queries_to_run = self.concurrent_queries_to_run - self.currently_running
            for i in range(0, new_queries_to_run):
                query = random.choice(ftsQueries)
                if bucket.loadDefn.get("valType") == "Hotel":
                    query = random.choice(HotelQueries)
                index, details = random.choice(bucket.ftsIndexes.items())
                _, b, s, _ = details
                self.total_query_count += 1
                threads.append(Thread(
                    target=self._run_query,
                    name="query_thread_{0}".format(self.total_query_count),
                    args=(index, query, b, s)))
            i = 0
            self.currently_running += new_queries_to_run
            for thread in threads:
                i += 1
                thread.start()

            time.sleep(2)
        if self.failed_count.next()-1>0 or self.error_count.next()-1 > 0:
            raise Exception("Queries Failed:%s , Queries Error Out:%s" %
                            (self.failed_count, self.error_count))

    def _run_query(self, index, query, b, s, validate_item_count=False, expected_count=0):
        start = time.time()
        try:
            result = self.execute_fts_query("{}".format(index), query)
            if validate_item_count:
                if result.metaData().metrics().totalRows() != expected_count:
                    self.failed_count.next()
                else:
                    self.success_count.next()
            else:
                self.success_count.next()
        except TimeoutException or AmbiguousTimeoutException or UnambiguousTimeoutException as e:
            self.timeout_count.next()
        except RequestCanceledException as e:
            self.cancel_count.next()
        except CouchbaseException as e:
            print(e)
            self.rejected_count.next()
        except Exception as e:
            print(e)
            self.error_count.next()
        end = time.time()
        if end - start < 1:
            time.sleep(end - start)
        self.currently_running -= 1

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
