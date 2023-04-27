import json
import random
from threading import Thread
import threading
import time

from sdk_client3 import SDKClient
from com.couchbase.client.java.query import QueryOptions,\
    QueryScanConsistency, QueryStatus
from com.couchbase.client.core.deps.io.netty.handler.timeout import TimeoutException
from com.couchbase.client.core.error import RequestCanceledException,\
    CouchbaseException, InternalServerFailureException,\
    AmbiguousTimeoutException, UnambiguousTimeoutException,\
    PlanningFailureException, IndexNotFoundException
from string import ascii_uppercase, ascii_lowercase
from encodings.punycode import digits
from gsiLib.gsiHelper import GsiHelper
import traceback
from global_vars import logger
from table_view import TableView
import itertools
from _collections import defaultdict
from membase.api.rest_client import RestConnection

letters = ascii_uppercase + ascii_lowercase + digits

queries = ['select name from {} where age between 30 and 50 limit 100;',
           'select name from {} where body is not null and age between 0 and 50 limit 100;',
           'select age, count(*) from {} where marital = "M" group by age order by age limit 100;',
           'select v.name, animal from {} as v unnest animals as animal where v.attributes.hair = "Burgundy" and animal is not null limit 100;',
           'SELECT v.name, ARRAY hobby.name FOR hobby IN v.attributes.hobbies END FROM {} as v WHERE v.attributes.hair = "Burgundy" and gender = "F" and ANY hobby IN v.attributes.hobbies SATISFIES hobby.type = "Music" END limit 100;',
           'select name, ROUND(attributes.dimensions.weight / attributes.dimensions.height,2) from {} WHERE gender is not MISSING limit 100;']

indexes = ['create index {}{} on {}(age) where age between 30 and 50 WITH {{ "defer_build": true}};',
           'create index {}{} on {}(body) where age between 0 and 50 WITH {{ "defer_build": true}};',
           'create index {}{} on {}(marital,age) WITH {{ "defer_build": true}};',
           'create index {}{} on {}(ALL `animals`,`attributes`.`hair`,`name`) where attributes.hair = "Burgundy" WITH {{ "defer_build": true}};',
           'CREATE INDEX {}{} ON {}(`gender`,`attributes`.`hair`, DISTINCT ARRAY `hobby`.`type` FOR hobby in `attributes`.`hobbies` END) where gender="F" and attributes.hair = "Burgundy" WITH {{ "defer_build": true}};',
           'create index {}{} on {}(`gender`,`attributes`.`dimensions`.`weight`, `attributes`.`dimensions`.`height`,`name`) WITH {{ "defer_build": true}};']

_HotelIndexes = ['CREATE INDEX {}{} ON {}(country, DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` FOR r in `reviews` END,array_count((`public_likes`)),array_count((`reviews`)) DESC,`type`,`phone`,`price`,`email`,`address`,`name`,`url`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`type`,`free_parking`,array_count((`public_likes`)),`price`,`country`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`free_parking`,`country`,`city`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`price`,`city`,`name`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(ALL ARRAY `r`.`ratings`.`Rooms` FOR r IN `reviews` END,`avg_rating`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`price`,`name`,`city`,`country`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`name` INCLUDE MISSING DESC,`phone`,`type`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(DISTINCT ARRAY FLATTEN_KEYS(`r`.`author`,`r`.`ratings`.`Cleanliness`) FOR r IN `reviews` when `r`.`ratings`.`Cleanliness` < 4 END, `country`, `email`, `free_parking`) USING GSI WITH {{ "defer_build": true}};']

_HotelQueries = ["select meta().id from {} where country is not null and `type` is not null and (any r in reviews satisfies r.ratings.`Check in / front desk` is not null end) limit 100",
                "select avg(price) as AvgPrice, min(price) as MinPrice, max(price) as MaxPrice from {} where free_breakfast=True and free_parking=True and price is not null and array_count(public_likes)>5 and `type`='Hotel' group by country limit 100",
                "select city,country,count(*) from {} where free_breakfast=True and free_parking=True group by country,city order by country,city limit 100",
                "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM {0} WHERE price IS NOT NULL GROUP BY city) SELECT h.name, h.price FROM {0} h JOIN city_avg ON h.city = city_avg.city WHERE h.price < city_avg.avgprice AND h.price IS NOT NULL limit 100",
                "SELECT h.name, h.city, r.author FROM {} h UNNEST reviews AS r WHERE r.ratings.Rooms < 2 AND h.avg_rating >= 3 ORDER BY r.author DESC limit 100",
                "SELECT COUNT(*) FILTER (WHERE free_breakfast = TRUE) AS count_free_breakfast, COUNT(*) FILTER (WHERE free_parking = TRUE) AS count_free_parking, COUNT(*) FILTER (WHERE free_breakfast = TRUE AND free_parking = TRUE) AS count_free_parking_and_breakfast FROM {} WHERE city LIKE 'North%' ORDER BY count_free_parking_and_breakfast DESC  limit 100",
                "SELECT h.name,h.country,h.city,h.price,DENSE_RANK() OVER (window1) AS `rank` FROM {} AS h WHERE h.price IS NOT NULL WINDOW window1 AS ( PARTITION BY h.country ORDER BY h.price NULLS LAST) limit 100",
                "SELECT * from {} where `type` is not null limit 100",
                "SELECT * from {} where phone like \"4%\" limit 100",
                "SELECT * FROM {} AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country IS NOT NULL limit 100"]


HotelIndexes = ['CREATE INDEX {}{} ON {}(country, DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` FOR r in `reviews` END,array_count((`public_likes`)),array_count((`reviews`)) DESC,`type`,`phone`,`price`,`email`,`address`,`name`,`url`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`type`,`free_parking`,array_count((`public_likes`)),`price`,`country`) PARTITION BY HASH (type) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`free_parking`,`country`,`city`)  PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`country`, `city`,`price`,`name`)  PARTITION BY HASH (country, city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(ALL ARRAY `r`.`ratings`.`Rooms` FOR r IN `reviews` END,`avg_rating`)  PARTITION BY HASH (avg_rating) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`price`,`name`,`city`,`country`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`name` INCLUDE MISSING DESC,`phone`,`type`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`country`, `free_parking`, DISTINCT ARRAY FLATTEN_KEYS(`r`.`ratings`.`Cleanliness`,`r`.`author`) FOR r IN `reviews` when `r`.`ratings`.`Cleanliness` < 4 END, `email`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};']


HotelQueries = ["select meta().id from {} where country is not null and `type` is not null and (any r in reviews satisfies r.ratings.`Check in / front desk` is not null end) limit 100",
                "select price, country from {} where free_breakfast=True AND free_parking=True and price is not null and array_count(public_likes)>=0 and `type`='Hotel' limit 100",
                "select city,country from {} where free_breakfast=True and free_parking=True order by country,city limit 100",
                "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM {0} WHERE country = 'Bulgaria' GROUP BY city limit 10) SELECT h.name, h.price FROM city_avg JOIN {0} h ON h.city = city_avg.city WHERE h.price < city_avg.avgprice AND h.country='Bulgaria' limit 100",
                "SELECT h.name, h.city, r.author FROM {} h UNNEST reviews AS r WHERE r.ratings.Rooms = 2 AND h.avg_rating >= 3 limit 100",
                "SELECT COUNT(1) AS cnt FROM {} WHERE city LIKE 'North%'",
                "SELECT h.name,h.country,h.city,h.price FROM {} AS h WHERE h.price IS NOT NULL limit 100",
                "SELECT * from {} where `name` is not null limit 100",
                "SELECT * from {} where phone like \"San%\" limit 100",
                "SELECT * FROM {} AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country = 'Bulgaria' limit 100"]


def execute_statement_on_n1ql(client, statement, client_context_id=None):
    """
    Executes a statement on CBAS using the REST API using REST Client
    """
    response = execute_via_sdk(client, statement, False, client_context_id)
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
    if "status" in response:
        status = response["status"]
    else:
        status = None
    return status, metrics, errors, results, handle


def execute_via_sdk(client, statement, readonly=False,
                    client_context_id=None):
    options = QueryOptions.queryOptions()
    options.scanConsistency(QueryScanConsistency.NOT_BOUNDED)
    options.readonly(readonly)
    if client_context_id:
        options.clientContextId(client_context_id)

    output = {}
    result = client.query(statement)

    output["status"] = result.metaData().status()
    output["metrics"] = result.metaData().metrics()

    try:
        output["results"] = result.rowsAsObject()
    except:
        output["results"] = None

    if str(output['status']) == QueryStatus.FATAL:
        msg = output['errors'][0]['msg']
        if "Job requirement" in msg and "exceeds capacity" in msg:
            raise Exception("Capacity cannot meet job requirement")
    elif output['status'] == QueryStatus.SUCCESS:
        output["errors"] = None
    else:
        raise Exception("N1QL query failed")

    return output


class DoctorN1QL():

    def __init__(self, cluster, bucket_util):
        self.bucket_util = bucket_util
        self.cluster = cluster
        self.sdkClients = dict()
        self.log = logger.get("test")
        self.stop_run = False

    def monitor_gsi_auto_scaling(self, dataplane_id):
        '''
        1. Monitor when the GSI scaling should trigger.
        2. Wait for GSI scaling to trigger
        3. Assert the number of GSI nodes in the cluster
        '''
        pass

    def discharge_N1QL(self):
        self.stop_run = True

    def create_indexes(self, buckets):
        for b in buckets:
            b.indexes = dict()
            b.queries = list()
            i = 0
            q = 0
            self.log.info("Creating GSI indexes on {}".format(b.name))
            if b.serverless and b.serverless.nebula_endpoint:
                self.cluster_conn = SDKClient([b.serverless.nebula_endpoint], None).cluster
            while i < b.loadDefn.get("2i")[0]:
                for s in self.bucket_util.get_active_scopes(b, only_names=True):
                    if b.name+s not in self.sdkClients.keys():
                        self.sdkClients.update({b.name+s: self.cluster_conn.bucket(b.name).scope(s)})
                        time.sleep(5)
                    for c in sorted(self.bucket_util.get_active_collections(b, s, only_names=True)):
                        if c == "_default":
                            continue
                        if i < b.loadDefn.get("2i")[0]:
                            indexType = indexes
                            queryType = queries
                            if b.loadDefn.get("valType") == "Hotel":
                                indexType = HotelIndexes
                                queryType = HotelQueries
                            self.idx_q = indexType[i % len(indexType)].format(b.name.replace("-", "_") + "_idx_" + c + "_", i, c)
                            query = queryType[i % len(indexType)].format(c)
                            print self.idx_q
                            print query
                            b.indexes.update({b.name.replace("-", "_") + "_idx_"+c+"_"+str(i): (self.idx_q, self.sdkClients[b.name+s], b.name, s, c)})
                            if q < b.loadDefn.get("2i")[1]:
                                b.queries.append((query, self.sdkClients[b.name+s]))
                                q += 1
                            retry = 5
                            while retry > 0:
                                try:
                                    execute_statement_on_n1ql(self.sdkClients[b.name+s], self.idx_q)
                                    break
                                except PlanningFailureException or CouchbaseException or UnambiguousTimeoutException or TimeoutException or AmbiguousTimeoutException or RequestCanceledException as e:
                                    print(e)
                                    retry -= 1
                                    time.sleep(10)
                                    continue
                                except IndexNotFoundException as e:
                                    print "Returning from here as we get IndexNotFoundException"
                                    print(e)
                                    return False
                            i += 1
        return True

    def build_indexes(self, buckets, dataplane_objs=None,
                      wait=False, timeout=86400):
        build = True
        i = 0
        while build:
            build = False
            k = 0
            while buckets[k*10:(k+1)*10]:
                _buckets = buckets[k*10:(k+1)*10]
                for bucket in _buckets:
                    d = defaultdict(list)
                    for key, val in bucket.indexes.items():
                        _, _, _, _, c = val
                        d[c].append(key)
                    for collection in sorted(d.keys())[i:i+1]:
                        details = bucket.indexes[d.get(collection)[0]]
                        build = True
                        build_query = "BUILD INDEX on `%s`(%s) USING GSI" % (
                            collection, ",".join(sorted(d.get(collection))))
                        time.sleep(1)
                        start = time.time()
                        while time.time() < start + 600:
                            try:
                                execute_statement_on_n1ql(details[1], build_query)
                                break
                            except Exception as e:
                                print(e)
                                break
                            except InternalServerFailureException as e:
                                print(e)
                                break
                            except PlanningFailureException as e:
                                print(e)
                                time.sleep(10)
                            except AmbiguousTimeoutException or UnambiguousTimeoutException as e:
                                print(e)
                                time.sleep(10)
                if wait:
                    for bucket in _buckets:
                        d = defaultdict(list)
                        for key, val in bucket.indexes.items():
                            _, _, _, _, c = val
                            d[c].append(key)
                        self.rest = GsiHelper(dataplane_objs[bucket.serverless.dataplane_id].master, logger["test"])
                        status = False
                        for collection in sorted(d.keys())[i:i+1]:
                            for index_name in sorted(d.get(collection)):
                                details = bucket.indexes[index_name]
                                status = self.rest.polling_create_index_status(
                                    bucket, index_name, timeout=timeout/10)
                                print("index: {}, status: {}".format(index_name, status))
                                if status is True:
                                    self.log.info("2i index is ready: {}".format(index_name))
                k += 1
            i += 1

    def drop_indexes(self, buckets):
        for index, details in buckets:
            build_query = "DROP INDEX %s on `%s`" % (index, details[4])
            execute_statement_on_n1ql(details[1], build_query)

    def index_stats(self, dataplanes):
        for dataplane in dataplanes.values():
            stat_monitor = threading.Thread(target=self.log_index_stats_new,
                                            kwargs=dict(dataplane=dataplane,
                                                        print_duration=60))
            stat_monitor.start()

    def index_ru_wu_stats(self, dataplanes):
        for dataplane in dataplanes.values():
            stat_monitor = threading.Thread(target=self.log_index_ru_wu_stats,
                                            kwargs=dict(dataplane=dataplane,
                                                        print_duration=60))
            stat_monitor.start()

    def query_stats(self, dataplanes):
        for dataplane in dataplanes.values():
            stat_monitor = threading.Thread(target=self.log_query_stats,
                                            kwargs=dict(dataplane=dataplane,
                                                        print_duration=60))
            stat_monitor.start()

    def log_index_stats(self, dataplane, print_duration=600):
        st_time = time.time()
        while not self.stop_run:
            if st_time + print_duration < time.time():
                self.table = TableView(self.log.info)
                self.table.set_headers(["Dataplane",
                                        "Node",
                                        "num_tenants",
                                        "num_indexes",
                                        "memory_used_actual",
                                        "units_used_actual/units_quota"])
                for node in dataplane.index_nodes:
                    try:
                        rest = RestConnection(node)
                        resp = rest.urllib_request(rest.indexUrl + "stats")
                        content = json.loads(resp.content)
                        self.table.add_row([
                            dataplane.id,
                            node.ip,
                            str(content["num_tenants"]),
                            str(content["num_indexes"]),
                            str(content["memory_used_actual"]),
                            "{}/{}".format(str(content["units_used_actual"]),
                                           str(content["units_quota"])),
                            ])
                    except Exception as e:
                        self.log.critical(e)
                self.table.display("Index Statistics")
                st_time = time.time()

    def log_index_ru_wu_stats(self, dataplane, print_duration=600):
        st_time = time.time()
        while not self.stop_run:
            if st_time + print_duration < time.time():
                self.table = TableView(self.log.info)
                for node in dataplane.index_nodes:
                    try:
                        rest = RestConnection(node)
                        resp = rest.urllib_request(rest.indexUrl + "_metering")
                        print("################## Index Node RU/WU Stats for node {} , Dataplane: {} ##################".format(node, dataplane.id))
                        content = resp.text
                        print(content)
                    except Exception as e:
                        self.log.critical(e)
                # self.table.display("Index RU - WU Statistics")
                st_time = time.time()

    def log_query_stats(self, dataplane, print_duration=600):
        st_time = time.time()
        self.n1ql_cooling = False
        while not self.stop_run:
            self.n1ql_nodes_below30 = 0
            self.n1ql_nodes_above60 = 0
            self.scale_up_n1ql = False
            self.scale_down_n1ql = False
            n1ql_table = TableView(self.log.info)
            n1ql_table.set_headers(["Dataplane",
                                    "Node",
                                    "load_factor",
                                    "queued",
                                    "active"])
            for node in dataplane.query_nodes:
                try:
                    rest = RestConnection(node)
                    resp = rest.urllib_request(rest.queryUrl + "admin/stats")
                    # vitals = rest.urllib_request(rest.queryUrl + "admin/vitals")
                    content = json.loads(resp.content)
                    n1ql_table.add_row([
                        dataplane.id,
                        node.ip,
                        str(content["load_factor.value"]),
                        str(content["queued_requests.value"]),
                        str(content["active_requests.value"])
                    ])
                    if content["load_factor.value"] >= 60:
                        self.n1ql_nodes_above60 += 1
                    elif content["load_factor.value"] < 30:
                        self.n1ql_nodes_below30 += 1
                except Exception as e:
                    self.log.critical(e)
            if self.scale_down_n1ql is False and self.scale_up_n1ql is False and self.n1ql_cooling is False:
                if self.n1ql_nodes_above60 == len(dataplane.query_nodes):
                    self.scale_up_n1ql = True
                elif self.n1ql_nodes_below30 == len(dataplane.query_nodes) and len(dataplane.query_nodes) >= 4:
                    self.scale_down_n1ql = True
            if st_time + print_duration < time.time():
                n1ql_table.display("Query Statistics")
                st_time = time.time()
            time.sleep(60)

    def log_index_stats_new(self, dataplane, print_duration=600):
        st_time = time.time()
        self.scale_down = False
        self.scale_up = False
        self.gsi_auto_rebl = False
        self.gsi_cooling = False
        self.last_30_mins = defaultdict(list)
        while not self.stop_run:
            self.nodes_below_LWM = 0
            self.nodes_above_LWM = 0
            self.nodes_above_HWM = 0
            self.scale_down_nodes = 0
            rest = RestConnection(dataplane.master)

            self.table = TableView(self.log.info)
            self.table.set_headers(["Node",
                                    "ServerGroup",
                                    "num_tenants",
                                    "num_indexes",
                                    "memory_used_actual",
                                    "units_used_actual/units_quota",
                                    "30 min Avg Mem",
                                    "30 min Avg Units"])
            pools = rest.get_pools_default()
            for node in dataplane.index_nodes:
                try:
                    rest = RestConnection(node)
                    resp = rest.urllib_request(rest.indexUrl + "stats")
                    content = json.loads(resp.content)
                    mem_q = content["memory_quota"]*1.0
                    units_q = content["units_quota"]*1.0
                    mem_used = content["memory_used_actual"]/mem_q * 100
                    units_used = content["units_used_actual"]/units_q * 100
                    if len(self.last_30_mins[node.ip]) > 30:
                        self.last_30_mins[node.ip].pop(0)
                    self.last_30_mins[node.ip].append((mem_used, units_used))
                    avg_mem_used = sum([consumption[0] for consumption in self.last_30_mins[node.ip]])/len(self.last_30_mins[node.ip])
                    avg_units_used = sum([consumption[1] for consumption in self.last_30_mins[node.ip]])/len(self.last_30_mins[node.ip])
                    if avg_mem_used > 70 or avg_units_used > 50:
                        self.nodes_above_HWM += 1
                    elif avg_mem_used > 45 or avg_units_used > 36:
                        self.nodes_above_LWM += 1
                    elif avg_mem_used < 40 or avg_units_used < 32:
                        self.nodes_below_LWM += 1

                    self.table.add_row([
                        node.ip,
                        [_node["serverGroup"] for _node in pools["nodes"] if node.ip in _node["hostname"]][0],
                        str(content["num_tenants"]),
                        str(content["num_indexes"]),
                        str(content["memory_used_actual"]),
                        "{}/{}".format(str(content["units_used_actual"]),
                                       str(content["units_quota"])),
                        avg_mem_used,
                        avg_units_used
                        ])
                except Exception as e:
                    self.log.critical(e)
            self.log.info("GSI - Nodes below LWM: {}".format(self.nodes_below_LWM))
            self.log.info("GSI - Nodes above LWM: {}".format(self.nodes_above_LWM))
            self.log.info("GSI - Nodes above HWM: {}".format(self.nodes_above_HWM))

            # Check for GSI Auto-rebalance
            try:
                self.defrag = rest.urllib_request(rest.baseUrl + "pools/default/services/index/defragmented")
                self.defrag = json.loads(self.defrag.content)
                self.defrag_table = TableView(self.log.info)
                self.defrag_table.set_headers(["Node",
                                               "num_tenants",
                                               "num_index_repaired",
                                               "memory_used_actual",
                                               "units_used_actual"])
                for node, gsi_stat in self.defrag.items():
                    self.defrag_table.add_row([node,
                                               gsi_stat["num_tenants"],
                                               gsi_stat["num_index_repaired"],
                                               gsi_stat["memory_used_actual"],
                                               gsi_stat["units_used_actual"]
                                               ])
            except Exception as e:
                self.defrag = dict()
                self.log.critical(e)

            if st_time + print_duration < time.time():
                self.table.display("Index Statistics")
                self.defrag_table.display("Index Defrag Stats")
                st_time = time.time()
            if self.scale_down is False and self.scale_up is False and self.gsi_auto_rebl is False and self.gsi_cooling is False:
                num_tenant_0 = 0
                nodes_below_15_tenants = 0
                nodes_below_LWM_defrag = 0
                for node, gsi_stat in self.defrag.items():
                    if gsi_stat["num_tenants"] == 0:
                        num_tenant_0 += 1
                    elif gsi_stat["num_tenants"] <= 15\
                        and gsi_stat["memory_used_actual"]/mem_q * 100 < 40\
                            and gsi_stat["units_used_actual"]/units_q * 100 < 32:
                        nodes_below_15_tenants += 1
                    if gsi_stat["memory_used_actual"]/mem_q * 100 < 40\
                            and gsi_stat["units_used_actual"]/units_q * 100 < 32:
                        nodes_below_LWM_defrag += 1
                    if gsi_stat["num_index_repaired"] > 0:
                        self.log.info("{} have indexes to be repaired".format(node))
                        self.gsi_auto_rebl = True
                        self.log.info("GSI - Auto-Rebalance should trigger in a while as num_index_repaired > 0")
                        continue
                self.log.info("Nodes below LWM from the defrag API: %s" % nodes_below_LWM_defrag)
                if num_tenant_0 > 0\
                    and nodes_below_15_tenants == len(dataplane.index_nodes) - num_tenant_0\
                        and len(dataplane.index_nodes) - num_tenant_0 >= 2:
                    self.scale_down = True
                    self.scale_down_nodes = num_tenant_0
                    self.log.info("GSI - Scale DOWN should trigger in a while")
                if self.nodes_above_HWM > 1 and self.nodes_below_LWM > 1:
                    if nodes_below_LWM_defrag == dataplane.index_nodes:
                        self.gsi_auto_rebl = True
                        self.log.info("GSI - Auto-Rebalance should trigger in a while")
                    elif len(dataplane.index_nodes) < 10:
                        self.scale_up = True
                        self.log.info("(RULE2) GSI - Scale UP should trigger in a while")
                if self.nodes_above_LWM == len(dataplane.index_nodes) or self.nodes_above_HWM == len(dataplane.index_nodes):
                    if len(dataplane.index_nodes) < 10:
                        self.scale_up = True
                        self.log.info("(RULE1) GSI - Scale UP should trigger in a while")
            time.sleep(60)


class QueryLoad:
    def __init__(self, bucket):
        self.bucket = bucket
        self.queries = bucket.queries
        self.failed_count = itertools.count()
        self.success_count = itertools.count()
        self.rejected_count = itertools.count()
        self.error_count = itertools.count()
        self.cancel_count = itertools.count()
        self.timeout_count = itertools.count()
        self.total_query_count = 0
        self.stop_run = False
        self.log = logger.get("infra")
        self.cluster_conn = None

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
        self.concurrent_queries_to_run = bucket.loadDefn.get("2i")[1]
        self.currently_running = 0
        query_count = 0
        for i in range(0, self.concurrent_queries_to_run):
            self.total_query_count += 1
            self.currently_running += 1
            query = random.choice(self.queries)
            self.cluster_conn = query[1]
            threads.append(Thread(
                target=self._run_query,
                name="query_thread_{0}".format(self.total_query_count),
                args=(query[1], query[0], False, 0)))

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
                query = random.choice(self.queries)
                self.total_query_count += 1
                threads.append(Thread(
                    target=self._run_query,
                    name="query_thread_{0}".format(self.total_query_count),
                    args=(query[1], query[0], False, 0)))
            i = 0
            self.currently_running += new_queries_to_run
            for thread in threads:
                i += 1
                thread.start()

            time.sleep(2)
        if self.failed_count.next()-1>0 or self.error_count.next()-1 > 0:
            raise Exception("Queries Failed:%s , Queries Error Out:%s" %
                            (self.failed_count, self.error_count))

    def _run_query(self, client, query, validate_item_count=False, expected_count=0):
        name = threading.currentThread().getName()
        client_context_id = name
        start = time.time()
        e = ""
        try:
            status, _, _, results, _ = execute_statement_on_n1ql(
                client, query, client_context_id=client_context_id)
            if status == QueryStatus.SUCCESS:
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
            self.timeout_count.next()
        except RequestCanceledException as e:
            self.cancel_count.next()
        except CouchbaseException as e:
            self.rejected_count.next()
        except Exception as e:
            print(e)
            self.error_count.next()
        if str(e).find("no more information available") != -1:
            self.log.critical(query)
            self.log.critical(e)
        end = time.time()
        if end - start < 1:
            time.sleep(end - start)
        self.currently_running -= 1

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
                self.table.display("N1QL Statistics")
                st_time = time.time()
