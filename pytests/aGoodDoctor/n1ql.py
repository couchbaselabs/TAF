'''
Created on 24-Apr-2021

@author: riteshagarwal
'''

import json
import random
from threading import Thread
import threading
import time

from com.couchbase.client.java.query import QueryOptions,\
    QueryScanConsistency, QueryStatus
from com.couchbase.client.core.deps.io.netty.handler.timeout import TimeoutException
from com.couchbase.client.core.error import RequestCanceledException,\
    CouchbaseException, InternalServerFailureException,\
    AmbiguousTimeoutException, PlanningFailureException,\
    UnambiguousTimeoutException, IndexNotFoundException, IndexExistsException
from string import ascii_uppercase, ascii_lowercase
from encodings.punycode import digits
from gsiLib.gsiHelper import GsiHelper
from global_vars import logger
from _collections import defaultdict
from table_view import TableView
import itertools
from com.couchbase.client.java.json import JsonObject
from com.github.javafaker import Faker
from constants.cb_constants.CBServer import CbServer
from connections.Rest_Connection import RestConnection
from com.couchbase.test.val import Hotel
import struct
from workloads import siftBigANN
import os
from _threading import Lock
from TestInput import TestInputSingleton
from java.net import SocketTimeoutException
from elasticsearch import EsClient
from java.util import HashMap
from com.couchbase.test.val import Vector

letters = ascii_uppercase + ascii_lowercase + digits
faker = Faker()

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
                "WITH city_price AS (SELECT city, price FROM {} WHERE country = 'Bulgaria' and price > 500 limit 1000) SELECT h.city, h.price FROM city_price as h;",
                "SELECT h.name, h.city, r.author FROM {} h UNNEST reviews AS r WHERE r.ratings.Rooms = 2 AND h.avg_rating >= 3 limit 100",
                "SELECT COUNT(1) AS cnt FROM {} WHERE city LIKE 'North%'",
                "SELECT h.name,h.country,h.city,h.price FROM {} AS h WHERE h.price IS NOT NULL limit 100",
                "SELECT * from {} where `name` is not null limit 100",
                "SELECT * from {} where phone like \"San%\" limit 100",
                "SELECT * FROM {} AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country = 'Bulgaria' limit 100"]

HotelIndexes = ['CREATE INDEX {}{} ON {}(country, DISTINCT ARRAY `r`.`ratings`.`Check in / front desk` FOR r in `reviews` END,array_count((`public_likes`)),array_count((`reviews`)) DESC,`type`,`phone`,`price`,`email`,`address`,`name`,`url`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`type`,`free_parking`,array_count((`public_likes`)),`price`,`country`) PARTITION BY HASH (type) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`free_breakfast`,`free_parking`,`country`,`city`)  PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`country`, `city`,`price`,`name`)  PARTITION BY HASH (country, city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(ALL ARRAY `r`.`ratings`.`Rooms` FOR r IN `reviews` END,`avg_rating`)  PARTITION BY HASH (avg_rating) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`price`,`name`,`city`,`country`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`name` INCLUDE MISSING DESC,`phone`,`type`) PARTITION BY HASH (name) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`country`, `free_parking`, DISTINCT ARRAY FLATTEN_KEYS(`r`.`ratings`.`Cleanliness`,`r`.`author`) FOR r IN `reviews` when `r`.`ratings`.`Cleanliness` < 4 END, `email`) PARTITION BY HASH (country) USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Inn USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Hostel USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Place USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Center USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Hotel USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Motel USING GSI WITH {{ "defer_build": true}};',
                'CREATE INDEX {}{} ON {}(`city` INCLUDE MISSING ASC, `phone`) PARTITION BY HASH (city) where type=Suites USING GSI WITH {{ "defer_build": true}};',]

HotelQueries = ["select meta().id from {} where country is not null and `type` is not null and (any r in reviews satisfies r.ratings.`Check in / front desk` is not null end) limit 100",
                "select price, country from {} where free_breakfast=True AND free_parking=True and price is not null and array_count(public_likes)>=0 and `type`= $type limit 100",
                "select city,country from {} where free_breakfast=True and free_parking=True order by country,city limit 100",
                "WITH city_avg AS (SELECT city, AVG(price) AS avgprice FROM {0} WHERE country = $country GROUP BY city limit 10) SELECT h.name, h.price FROM city_avg JOIN {0} h ON h.city = city_avg.city WHERE h.price < city_avg.avgprice AND h.country=$country limit 100",
                "SELECT h.name, h.city, r.author FROM {} h UNNEST reviews AS r WHERE r.ratings.Rooms = 2 AND h.avg_rating >= 3 limit 100",
                "SELECT COUNT(1) AS cnt FROM {} WHERE city LIKE 'North%'",
                "SELECT h.name,h.country,h.city,h.price FROM {} AS h WHERE h.price IS NOT NULL limit 100",
                "SELECT * from {} where `name` is not null limit 100",
                "SELECT * from {} where city like \"San%\" limit 100",
                "SELECT * FROM {} AS d WHERE ANY r IN d.reviews SATISFIES r.author LIKE 'M%' AND r.ratings.Cleanliness = 3 END AND free_parking = TRUE AND country = $country limit 100"]

HotelQueriesParams = [{},
                      {"type": "random.choice(['Inn', 'Hostel', 'Place', 'Center', 'Hotel', 'Motel', 'Suites'])"},
                      {},
                      {"country": "faker.address().country()"},
                      {},
                      {},
                      {},
                      {},
                      {},
                      {"country": "faker.address().country()"}
                      ]

NimbusPQueriesParams = [{"uid":"str(random.randint(0,1000000)).zfill(32)", "N":"random.randint(100, 1000)"},
                        {"conversationId1":"str(random.randint(0,1000000)).zfill(36)", "conversationId2":"str(random.randint(0,1000000)).zfill(36)"},
                        {"uid":"str(random.randint(0,1000000)).zfill(36)"}]

NimbusMQueriesParams = [{"conversationId":"str(random.randint(0,1000000)).zfill(36)", "N":"random.randint(100, 1000)"},
                        {"conversationId":"str(random.randint(0,1000000)).zfill(36)"}]

HotelQueriesVectorParams = [
    {"country": "faker.address().country()"},
    {"type": "random.choice(['Inn', 'Hostel', 'Place', 'Center', 'Hotel', 'Motel', 'Suites'])"},
    {},
    {},
    {},
    {},
    {},
    {"country": "faker.address().country()", "city": "faker.address().city()"}
    ]

SIFTQueriesVectorParams = [{}] * 10

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

def execute_via_sdk(client, statement, readonly=False,
                    client_context_id=None,
                    query_params=None, validate=True):
    options = QueryOptions.queryOptions()
    options.scanConsistency(QueryScanConsistency.NOT_BOUNDED)
    options.readonly(readonly)
    options.metrics(True)
    if client_context_id:
        options.clientContextId(client_context_id)
    if query_params:
        options.parameters(query_params)

    output = {}
    result = client.query(statement, options)
    output["status"] = result.metaData().status()
    output["metrics"] = result.metaData().metrics().get()

    if validate:
        try:
            output["results"] = result.rowsAsObject()
        except:
            output["results"] = result.rows()

    if str(output['status']) == QueryStatus.FATAL:
        msg = output['errors'][0]['msg']
        if "Job requirement" in msg and "exceeds capacity" in msg:
            raise Exception("Capacity cannot meet job requirement")
    elif output['status'] == QueryStatus.SUCCESS:
        output["errors"] = None
    else:
        raise Exception("N1QL query failed")

    return output

def performKNNSearchES(esClient, collection, qVec, k, gt, es_params=None):
    try:
        esResult = esClient.performKNNSearch(collection.lower().replace("_", ""), "embedding", qVec, k, es_params);
    except SocketTimeoutException as e:
        esClient = EsClient(esClient.serverUrl, esClient.apiKey)
        esClient.initializeSDK()
        print e
    esResult = json.loads(str(esResult).encode().decode())
    recall = 0
    all_ids_elastic = list()
    for hit in esResult["hits"]["hits"]:
        all_ids_elastic.append(hit["_source"]["id"])
    for item in all_ids_elastic:
        if item in gt[:100]:
            recall += 1
    return round(100.0 * recall/k, 2), esResult["took"]

class DoctorN1QL():

    def __init__(self, bucket_util):
        self.bucket_util = bucket_util
        self.sdkClients = dict()
        self.log = logger.get("test")
        self.stop_run = False
        self.query_failure = False

    def create_indexes(self, buckets, skip_index=False, base64=False, xattr=False):
        for b in buckets:
            # counter = 0
            b.indexes = dict()
            b.queries = list()
            b.query_map = dict()
            vector_defn_counter = 0
            self.log.info("Creating GSI indexes on {}".format(b.name))
            # prev_valType = None
            for s in self.bucket_util.get_active_scopes(b, only_names=True):
                if s == CbServer.system_scope:
                    continue
                if b.name+s not in self.sdkClients.keys():
                    self.sdkClients.update({b.name+s: b.clients[0].bucketObj.scope(s)})
                    time.sleep(1)
                collections = b.scopes[s].collections.keys()
                if "_default" in collections:
                    collections.remove("_default")
                for collection_num, c in enumerate(sorted(collections)):
                    if c == CbServer.default_collection:
                        continue
                    workloads = b.loadDefn.get("collections_defn", [b.loadDefn])
                    coll_order = TestInputSingleton.input.param("coll_order", 1)
                    if coll_order == -1:
                        workloads = list(reversed(workloads))
                    workload = workloads[collection_num % len(workloads)]
                    valType = workload["valType"]
                    indexType = workload.get("indexes")
                    queryType = workload.get("queries")
                    queryParams = []
                    # if valType != prev_valType:
                    #     counter = 0
                    if valType == "Hotel" and not workload.get("vector"):
                        indexType = HotelIndexes
                        queryType = HotelQueries
                        queryParams = HotelQueriesParams
                        # prev_valType = valType
                    if valType == "NimbusP":
                        queryParams = NimbusPQueriesParams
                        # prev_valType = valType
                    if valType == "NimbusM":
                        queryParams = NimbusMQueriesParams
                        # prev_valType = valType
                    if valType == "Hotel" and workload.get("vector"):
                        queryParams = HotelQueriesVectorParams
                        # prev_valType = valType
                    if valType == "siftBigANN":
                        if base64:
                            indexType = workload.get("indexes_base64")
                            queryType = workload.get("queries_base64")
                        elif TestInputSingleton.input.param("bhive", False):
                            indexType = workload.get("bhive_indexes")
                            queryType = workload.get("queries")
                        else:
                            indexType = workload.get("indexes")
                            queryType = workload.get("queries")
                        if xattr:
                            indexType = [index.replace("`embedding`", "meta().xattrs.embedding") for index in indexType]
                            queryType = [query.replace("embedding", "meta().xattrs.embedding") for query in queryType]
                        queryParams = SIFTQueriesVectorParams
                        # prev_valType = valType
                    i = 0
                    q = 0
                    while i < workload.get("2i")[0] or q < workload.get("2i")[1]:
                        if i < workload.get("2i")[0]:
                            vector_defn = {}
                            dim = None
                            if workload.get("vector"):
                                vector_defn = workload.get("vector")[vector_defn_counter % len(workload.get("vector"))]
                                dim = workload.get("dim")
                                similarity = vector_defn["similarity"]
                                nProbe = vector_defn["nProbe"]
                                description = vector_defn.get("description", "IVF,%s" % vector_defn.get("quantization", "SQ8"))
                                vector_fields = "'dimension': {}, 'description': '{}', 'similarity': '{}', 'scan_nprobes': {}".format(
                                dim, description, similarity, nProbe)
                                idx_name = b.name.replace("-", "_") + "_idx_" + c + "_" + str(i)
                                partitions = TestInputSingleton.input.param("partitions", None)
                                if partitions:
                                    vector_fields = "'num_partition': {}, ".format(partitions) + vector_fields
                                    self.idx_q = indexType[i % len(indexType)].format(
                                        index_name=idx_name, collection=c, vector=vector_fields)
                                else:
                                    self.idx_q = indexType[i % len(indexType)].format(
                                        index_name=idx_name, collection=c, vector=vector_fields)
                                    self.idx_q = self.idx_q.replace('PARTITION BY HASH(meta().id)', "")
                            else:
                                self.idx_q = indexType[i % len(indexType)].format(b.name.replace("-", "_") + "_idx_" + c + "_", i, c)
                            print self.idx_q
                            b.indexes.update({b.name.replace("-", "_") + "_idx_"+c+"_"+str(i): (self.idx_q, self.sdkClients[b.name+s], b.name, s, c)})
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
                                    except IndexExistsException:
                                        break
                            i += 1
                            vector_defn_counter += 1
                        if q < workload.get("2i")[1]:
                            gt = None
                            unformatted_q = queryType[q % len(queryType)]
                            if workload.get("vector"):
                                gt = workload.get("groundTruths")
                                query = unformatted_q.format(collection=c, similarity=similarity, nProbe=nProbe)
                            else:
                                query = unformatted_q.format(c)
                            print query
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
                                if workload.get("es"):
                                    b.query_map[query].update({"es": workload.get("es")[q]})
                                # b.queries.append((query, self.sdkClients[b.name+s], unformatted_q, vector_defn, dim))
                            q += 1
                        # counter += 1

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
                    for collection in sorted(d.keys())[i:i+build_count]:
                        details = bucket.indexes[d.get(collection)[0]]
                        build = True
                        build_query = "BUILD INDEX on `%s`(%s) USING GSI" % (
                            collection, ",".join(sorted(d.get(collection))))
                        # time.sleep(1)
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
                        self.rest = GsiHelper(cluster.master, logger["test"])
                        ths = list()
                        for collection in sorted(d.keys())[i:i+build_count]:
                            for index_name in sorted(d.get(collection)):
                                details = bucket.indexes[index_name]
                                th = threading.Thread(target=self.rest.polling_create_index_status,
                                                      name=index_name,
                                                      args=(bucket, index_name, timeout/10))
                                th.start()
                                ths.append(th)
                        for th in ths:
                            th.join()
                k += 1
            i += build_count

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
                update_q = "UPDATE STATISTICS FOR INDEX {0}.{1}".format(idx_details[4], idx_name)
                execute_statement_on_n1ql(self.sdkClients[idx_details[2]+idx_details[3]], update_q)
            time.sleep(interval)

    def start_update_stats(self, cluster):
        self.stop_run = False
        th = threading.Thread(target=self.update_index_stats,
                              kwargs=dict({"cluster": cluster}))
        th.start()

class QueryLoad:
    groundTruths = {}
    queryVectors = []

    def __init__(self, bucket, mockVector=True, validate_item_count=False, esClient=None, log_fail=True):
        self.mockVector = mockVector
        self.base64 = False
        self.validate_item_count = validate_item_count
        self.bucket = bucket
        self.queries = [item[0] for item in sorted(bucket.query_map.items(), key=lambda x:x[1]["identifier"])]
        self.queries_meta = [item[1] for item in sorted(bucket.query_map.items(), key=lambda x:x[1]["identifier"])]
        if not mockVector:
            for meta in self.queries_meta:
                gt = meta["gt"][0]
                if gt not in QueryLoad.groundTruths.keys():
                    QueryLoad.groundTruths.update({gt: self.read_gt_vecs(os.path.join(siftBigANN.get("baseFilePath"), gt))})
            self.query_stats = {query: [0, 0, 0, 0, self.queries_meta[idx]["gt"][1], Lock(), 0, 0] for idx, query in enumerate(self.queries)}
        else:
            self.query_stats = {query: [0, 0, 0, 0, "None", Lock(), 0, 0] for idx, query in enumerate(self.queries)}
        self.failed_count = itertools.count()
        self.success_count = itertools.count()
        self.rejected_count = itertools.count()
        self.error_count = itertools.count()
        self.cancel_count = itertools.count()
        self.timeout_count = itertools.count()
        self.total_query_count = itertools.count()
        self.stop_run = False
        self.log = logger.get("infra")
        self.cluster_conn = self.queries_meta[0]["sdk"]
        self.concurrent_queries_to_run = self.bucket.loadDefn.get("2iQPS")
        self.failures = 0
        self.timeout_failures = 0
        self.lock = Lock()
        self.esClient = esClient
        self.log_fail = log_fail

    def start_query_load(self):
        if self.bucket.loadDefn.get("valType") == "siftBigANN":
            self.query_stats = {query: [0, 0, 0, 0, self.queries_meta[idx]["gt"][1], Lock(), 0, 0, []] for idx, query in enumerate(self.queries)}
        else:
            self.query_stats = {query: [0, 0, 0, 0, "None", Lock(), 0, 0, []] for idx, query in enumerate(self.queries)}
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
        if self.bucket.loadDefn.get("collections_defn")[0].get("vector"):
            method = self._run_vector_query
            if not self.mockVector and not QueryLoad.queryVectors:
                QueryLoad.queryVectors = self.read_query_vecs(siftBigANN.get("baseFilePath") + "/bigann_query.bvecs")
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
        hotel = Vector(None)
        while not self.stop_run:
            # self.lock.acquire()
            client_context_id = name + str(counter)
            start = time.time()
            e = ""
            try:
                self.total_query_count.next()
                query = self.queries[counter%len(self.queries)]
                query_tuple = self.queries_meta[counter%len(self.queries)]
                # lock = self.query_stats[query][5]
                # lock.acquire()
                dim = query_tuple["dim"]
                if self.esClient:
                    es_scalars = query_tuple["es"]
                    es_params = []
                    for k, v in es_scalars.items():
                        scalar = HashMap()
                        scalar.put(k, v)
                        term = HashMap()
                        term.put("term", scalar)
                        es_params.append(term)
                vector_float = []
                if self.mockVector:
                    flt_buf = hotel.flt_buf
                    _slice = random.randint(0, hotel.flt_buf_length-dim)
                    embedding = flt_buf[_slice: _slice+dim]
                else:
                    index = self.query_stats[query][1] % len(QueryLoad.queryVectors)
                    gt = query_tuple["gt"][0]
                    expected_count = 10
                    embedding = QueryLoad.queryVectors[index]
                    groudtruth = QueryLoad.groundTruths[gt][index][:expected_count]

                if self.base64:
                    vector_float = hotel.convertToBase64Bytes(embedding)
                else:
                    for value in embedding:
                        vector_float.append(float(value))

                q_param = query_tuple["queryParams"]
                q_param.update({"vector": vector_float})
                q_param_json = JsonObject.create()
                for k, v in q_param.items():
                    try:
                        q_param_json.put(k, eval(v))
                    except:
                        q_param_json.put(k, v)
                # import pydevd
                # pydevd.settrace(trace_only_current_thread=False)
                status, metrics, _, results, _ = execute_statement_on_n1ql(
                    self.cluster_conn, query, client_context_id,
                    q_param_json, validate=validate_item_count)
                self.query_stats[query][0] += metrics.executionTime().toNanos()/1000000.0
                self.query_stats[query][1] += 1
                self.query_stats[query][8].append(metrics.executionTime().toNanos()/1000000.0)
                if len(self.query_stats[query][8]) > 1000:
                    self.query_stats[query][8].pop(0)
                # lock.release()
                if status == QueryStatus.SUCCESS:
                    if self.esClient:
                        recall_es, latency_es = performKNNSearchES(self.esClient, query_tuple["collection"], vector_float, 100, groudtruth, es_params)
                        self.query_stats[query][6] += latency_es
                        self.query_stats[query][7] += recall_es
                    if self.bucket.loadDefn.get("valType") == "siftBigANN" and validate_item_count:
                        results = [result.get("id") for result in list(results)]
                        if len(results) != expected_count:
                            self.log.critical("Expected Count:{}, Actual Count:{}".format(expected_count, len(results)))
                            self.failed_count.next()
                        else:
                            self.success_count.next()
                            accuracy, recall = self.compare_result(results, groudtruth)
                            self.query_stats[query][2] += accuracy
                            self.query_stats[query][3] += round(100.0 * recall//len(results), 2)
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
            # if e:
            #     lock.release()
            if str(e).find("TimeoutException") != -1\
                or str(e).find("AmbiguousTimeoutException") != -1\
                    or str(e).find("UnambiguousTimeoutException") != -1:
                self.timeout_failures += self.timeout_count.next()
                if self.timeout_failures % 50 == 0 or str(e).find("UnambiguousTimeoutException") != -1:
                    self.log.critical(client_context_id + ":" + query)
                    self.log.critical(e)
            elif str(e).find("RequestCanceledException") != -1:
                self.failures += self.cancel_count.next()
            elif str(e).find("InternalServerFailureException") != -1 or str(e).find("CouchbaseException") != -1:
                self.failures += self.error_count.next()

            if self.log_fail and e and (str(e).find("no more information available") != -1 or str(e).find("Index not ready for serving queries") != -1):
                self.log.critical(client_context_id + ":" + query)
                self.log.critical(e)
            end = time.time()
            # if end - start < 1:
            #     time.sleep(end - start)
            counter += 1

    def _run_query(self, validate_item_count=False, expected_count=0):
        name = threading.currentThread().getName()
        counter = 0
        while not self.stop_run:
            client_context_id = name + "__" + str(counter)
            counter += 1
            start = time.time()
            e = ""
            try:
                self.total_query_count.next()
                query = self.queries[counter%len(self.queries)]
                query_tuple = self.queries_meta[counter%len(self.queries)]
                q_param = query_tuple["queryParams"]
                q_param_json = JsonObject.create()
                for k, v in q_param.items():
                    try:
                        q_param_json.put(k, eval(v))
                    except:
                        q_param_json.put(k, v)
                status, metrics, _, results, _ = execute_statement_on_n1ql(
                    self.cluster_conn, query, client_context_id,
                    q_param_json, validate=validate_item_count)
                exec_time = metrics.executionTime().toNanos()/1000000.0
                self.query_stats[query][0] += exec_time
                self.query_stats[query][1] += 1
                self.query_stats[query][8].append(exec_time)
                if len(self.query_stats[query][8]) > 1000:
                    self.query_stats[query][8].pop(0)
                if status == QueryStatus.SUCCESS:
                    if validate_item_count:
                        if results[0]['$1'] != expected_count:
                            self.failed_count.next()
                            print q_param_json
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
                self.timeout_failures += self.timeout_count.next()
                self.log.critical(client_context_id + ":" + query)
                self.log.critical(e)
                if self.timeout_failures % 50 == 0 or str(e).find("UnambiguousTimeoutException") != -1:
                    self.log.critical(client_context_id + ":" + query)
                    self.log.critical(e)
            elif str(e).find("RequestCanceledException") != -1:
                self.failures += self.cancel_count.next()
            elif str(e).find("InternalServerFailureException") != -1 or str(e).find("CouchbaseException") != -1:
                self.failures += self.error_count.next()

            if e and (str(e).find("AmbiguousTimeoutException") == -1 or str(e).find("no more information available") != -1):
                self.log.critical(client_context_id + ":" + query)
                self.log.critical(e)
            end = time.time()
            # if end - start < 1:
            #     time.sleep(end - start)

    def read_query_vecs(self, filename):
        '''
            4+d bytes for .bvecs
            4+d*4 bytes for .fvecs and .ivecs
            File: bigann_query.bvecs
        '''
        groundTruths = []
        f = open(filename, '+rb')

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
        f = open(filename, '+rb')

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
