'''
Created on 24-Apr-2021

@author: riteshagarwal
'''

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
    AmbiguousTimeoutException, PlanningFailureException,\
    UnambiguousTimeoutException, IndexNotFoundException
from string import ascii_uppercase, ascii_lowercase
from encodings.punycode import digits
from gsiLib.gsiHelper import GsiHelper
from global_vars import logger
from _collections import defaultdict
from table_view import TableView
import itertools

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

_HotelIndexes = ['CREATE INDEX {}{} ON {}(country, DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` FOR r in `reviews` END,array_count((`public_likes`)),array_count((`reviews`)) DESC,`type`,`phone`,`price`,`email`,`address`,`name`,`url`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`type`,`free_parking`,array_count((`public_likes`)),`price`,`country`) PARTITION BY HASH (type) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`free_parking`,`country`,`city`)  PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`price`,`city`,`name`)  PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(ALL ARRAY `r`.`ratings`.`Rooms` FOR r IN `reviews` END,`avg_rating`)  PARTITION BY HASH (avg_rating) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`city`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`price`,`name`,`city`,`country`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`name` INCLUDE MISSING DESC,`phone`,`type`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(DISTINCT ARRAY FLATTEN_KEYS(`r`.`author`,`r`.`ratings`.`Cleanliness`) FOR r IN `reviews` when `r`.`ratings`.`Cleanliness` < 4 END, `country`, `email`, `free_parking`) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};']

_HotelQueries = ["select meta().id from {} where country is not null and `type` is not null and (any r in reviews satisfies r.ratings.`Check in / front desk` is not null end) limit 100",
                "select avg(price) as AvgPrice, min(price) as MinPrice, max(price) as MaxPrice from {} where free_breakfast=True and free_parking=True and price is not null and array_count(public_likes)>5 and `type`='Hotel' group by country limit 100",
                "select city,country,count(*) from {} where free_breakfast=True and free_parking=True group by country,city order by country,city limit 100",
                "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM {0} WHERE price IS NOT NULL GROUP BY city) SELECT h.name, h.price FROM {0} h JOIN city_avg ON h.city = city_avg.city WHERE h.price < city_avg.avgprice AND h.price IS NOT NULL limit 100",
                "SELECT h.name, h.city, r.author FROM {} h UNNEST reviews AS r WHERE r.ratings.Rooms < 2 AND h.avg_rating >= 3 limit 100",
                "SELECT COUNT(*) FILTER (WHERE free_breakfast = TRUE) AS count_free_breakfast, COUNT(*) FILTER (WHERE free_parking = TRUE) AS count_free_parking, COUNT(*) FILTER (WHERE free_breakfast = TRUE AND free_parking = TRUE) AS count_free_parking_and_breakfast FROM {} WHERE city LIKE 'North%' ORDER BY count_free_parking_and_breakfast DESC  limit 100",
                "SELECT h.name,h.country,h.city,h.price,DENSE_RANK() OVER (window1) AS `rank` FROM {} AS h WHERE h.price IS NOT NULL WINDOW window1 AS ( PARTITION BY h.country ORDER BY h.price NULLS LAST) limit 100",
                "SELECT * from {} where `type` is not null limit 100",
                "SELECT * from {} where phone like \"4%\" limit 100",
                "SELECT * FROM {} AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country IS NOT NULL limit 100"]

HotelIndexes = ['CREATE INDEX {}{} ON {}(country, DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` FOR r in `reviews` END,array_count((`public_likes`)),array_count((`reviews`)) DESC,`type`,`phone`,`price`,`email`,`address`,`name`,`url`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`type`,`free_parking`,array_count((`public_likes`)),`price`,`country`) PARTITION BY HASH (type) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`free_parking`,`country`,`city`)  PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`country`, `city`,`price`,`name`)  PARTITION BY HASH (country, city) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(ALL ARRAY `r`.`ratings`.`Rooms` FOR r IN `reviews` END,`avg_rating`)  PARTITION BY HASH (avg_rating) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`city`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`price`,`name`,`city`,`country`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`name` INCLUDE MISSING DESC,`phone`,`type`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};',
                'CREATE INDEX {}{} ON {}(`country`, `free_parking`, DISTINCT ARRAY FLATTEN_KEYS(`r`.`ratings`.`Cleanliness`,`r`.`author`) FOR r IN `reviews` when `r`.`ratings`.`Cleanliness` < 4 END, `email`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true, "num_replica":1, "num_partition":8}};']


HotelQueries = ["select meta().id from {} where country is not null and `type` is not null and (any r in reviews satisfies r.ratings.`Check in / front desk` is not null end) limit 100",
                "select price, country from {} where free_breakfast=True AND free_parking=True and price is not null and array_count(public_likes)>=0 and `type`='Hotel' limit 100",
                "select city,country from {} where free_breakfast=True and free_parking=True order by country,city limit 100",
                "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM {0} WHERE country = 'Bulgaria' GROUP BY city limit 10) SELECT h.name, h.price FROM city_avg JOIN {0} h ON h.city = city_avg.city WHERE h.price < city_avg.avgprice AND h.country='Bulgaria' limit 100",
                "SELECT h.name, h.city, r.author FROM {} h UNNEST reviews AS r WHERE r.ratings.Rooms = 2 AND h.avg_rating >= 3 limit 100",
                "SELECT COUNT(1) AS cnt FROM {} WHERE city LIKE 'North%'"
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

        self.sdkClient = SDKClient(cluster.query_nodes, None)
        self.cluster_conn = self.sdkClient.cluster
        self.stop_run = False
        self.query_failure = False

    def create_indexes(self, buckets, skip_index=False):
        for b in buckets:
            b.indexes = dict()
            b.queries = list()
            i = 0
            q = 0
            self.log.info("Creating GSI indexes on {}".format(b.name))
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
                            if not skip_index:
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

    def discharge_N1QL(self):
        self.stop_run = True

    def query_result(self):
        return self.query_failure

    def build_indexes(self, buckets,
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
                        self.rest = GsiHelper(self.cluster.master, logger["test"])
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

    def drop_indexes(self):
        for index, details in self.indexes.items():
            build_query = "DROP INDEX %s on `%s`" % (index, details[4])
            self.execute_statement_on_n1ql(details[1], build_query)


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
            print(e)
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
