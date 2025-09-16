'''
Created on 15-Apr-2021

@author: riteshagarwal
'''
from datetime import timedelta
import itertools
import json
import random
from threading import Thread
import threading
import time

from global_vars import logger
from table_view import TableView
from couchbase.analytics import AnalyticsScanConsistency, AnalyticsStatus
from couchbase.exceptions import (TimeoutException, UnAmbiguousTimeoutException, AmbiguousTimeoutException,
                                  RequestCanceledException, CouchbaseException, InternalServerFailureException)
from couchbase.options import AnalyticsOptions
from couchbase_columnar.options import QueryOptions
from datetime import timedelta

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

HeterogeneousQueries = [
    """SELECT COUNT(*) as total_documents,
       COUNT(key_0) as has_key_0,
       COUNT(key_1) as has_key_1, 
       COUNT(key_2) as has_key_2,
       COUNT(key_3) as has_key_3,
       COUNT(key_4) as has_key_4,
       AVG(OBJECT_LENGTH(0)) as avg_fields_per_doc
    FROM {0};""",

    """SELECT "key_0" as key_name,
       COUNT(CASE WHEN is_string(key_0) THEN 1 END) as string_count,
       COUNT(CASE WHEN is_number(key_0) THEN 1 END) as number_count,
       COUNT(CASE WHEN is_boolean(key_0) THEN 1 END) as boolean_count,
       COUNT(CASE WHEN is_array(key_0) THEN 1 END) as array_count,
       COUNT(CASE WHEN is_object(key_0) THEN 1 END) as object_count,
       COUNT(CASE WHEN is_null(key_0) THEN 1 END) as null_count
    FROM {0}
    WHERE key_0 IS NOT MISSING

    UNION ALL

    SELECT "key_1" as key_name,
        COUNT(CASE WHEN is_string(key_1) THEN 1 END),
        COUNT(CASE WHEN is_number(key_1) THEN 1 END),
        COUNT(CASE WHEN is_boolean(key_1) THEN 1 END),
        COUNT(CASE WHEN is_array(key_1) THEN 1 END),
        COUNT(CASE WHEN is_object(key_1) THEN 1 END),
        COUNT(CASE WHEN is_null(key_1) THEN 1 END)
    FROM {0}
    WHERE key_1 IS NOT MISSING

    UNION ALL

    SELECT "key_2" as key_name,
        COUNT(CASE WHEN is_string(key_2) THEN 1 END),
        COUNT(CASE WHEN is_number(key_2) THEN 1 END),
        COUNT(CASE WHEN is_boolean(key_2) THEN 1 END),
        COUNT(CASE WHEN is_array(key_2) THEN 1 END),
        COUNT(CASE WHEN is_object(key_2) THEN 1 END),
        COUNT(CASE WHEN is_null(key_2) THEN 1 END)
    FROM {0}
    WHERE key_2 IS NOT MISSING;""",
    
    """SELECT SUBSTR(string_val, 1, 6) as pattern,
       COUNT(*) as frequency,
       MIN(TONUMBER(SUBSTR(string_val, 7))) as min_number,
       MAX(TONUMBER(SUBSTR(string_val, 7))) as max_number,
       AVG(TONUMBER(SUBSTR(string_val, 7))) as avg_number
    FROM {0} as ds
    UNNEST [ds.key_0, ds.key_1, ds.key_2, ds.key_3, ds.key_4] as string_val
    WHERE is_string(string_val) 
    AND string_val LIKE "value_%"
    GROUP BY SUBSTR(string_val, 1, 6);
    """
    
]

def execute_statement_on_cbas(client, statement,
                              client_context_id=None):
    """
    Executes a statement on CBAS using the REST API using REST Client
    """
    try:
        response = execute_via_columnar_sdk(client, statement, False, client_context_id)

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

        if "metrics" in response:
            metrics = response["metrics"]
        else:
            metrics = None
        return metrics, errors, results

    except Exception as e:
        raise e


def execute_via_sdk(client, statement, readonly=False,
                    client_context_id=None):
    options = AnalyticsOptions(scan_consistency=AnalyticsScanConsistency.NOT_BOUNDED, readonly=readonly,
                               client_context_id=client_context_id)
    output = {}
    try:
        result = client.analytics_query(statement, options)
        output["results"] = [row for row in result.rows()]
        output["status"] = result.metadata().status()
        output["metrics"] = result.metadata().metrics()
    except CouchbaseException as e:
        # print(e)
        raise e
    except Exception as e:
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

def execute_via_columnar_sdk(client, statement, readonly=False, client_context_id=None):
    output = {}
    try:
        if client_context_id:
            options = QueryOptions(raw={'client_context_id':client_context_id}, metrics=True,timeout=timedelta(seconds=1200))
        else:
            options = QueryOptions(timeout=timedelta(seconds=1200), metrics=True)
        result = client.execute_query(statement, options)
        output["results"] = [row for row in result.rows()]
        output["metrics"] = result.metadata().metrics()
    except CouchbaseException as e:
        # print(e)
        raise e
    except Exception as e:
        raise e
    return output


class DoctorCBAS():

    def __init__(self):
        self.log = logger.get("test")
        self.stop_run = False

    def create_links(self, cluster, data_sources):
        for dataSource in data_sources:
            query_count = 0
            dataSource.create_link(cluster)
            if dataSource.loadDefn["valType"] == "Hotel":
                queryType = HotelQueries
            elif dataSource.loadDefn["valType"] == "RandomlyNestedJson":
                queryType = HeterogeneousQueries
            # create collection
            collections = dataSource.create_cbas_collections(cluster, dataSource.loadDefn.get("cbas")[0])
            q = 0
            while q < dataSource.loadDefn.get("cbas")[1]:
                coll = collections[q % dataSource.loadDefn.get("cbas")[0]]
                if queryType[q % len(queryType)] + dataSource.link_name in dataSource.query_map.keys():
                    q += 1
                    continue
                query = queryType[q % len(queryType)].format(coll)
                print(query)
                dataSource.query_map[queryType[q % len(queryType)] + dataSource.link_name] = ["Q%s_%s" % (query_count, coll)]
                query_count += 1
                dataSource.cbas_queries.append((query, queryType[q % len(queryType)] + dataSource.link_name))
                q += 1
            if dataSource.type != "s3":
                self.connect_link(cluster, dataSource.link_name)

            query_tbl = TableView(self.log.info)
            query_tbl.set_headers(["Bucket", "##", "Query"])
            for k, v in dataSource.query_map.items():
                query_tbl.add_row([dataSource.name, v[0], k])
            query_tbl.display("N1QL Queries to run during test:")

    def connect_link(self, cluster, link_name):
        client = cluster.SDKClients[0]
        statement = "CONNECT LINK %s" % link_name
        execute_statement_on_cbas(client, statement)
        self.log.info("Connect link %s is %s" % (link_name, "success"))

    def drop_links(self, cluster, databases):
        client = cluster.SDKClients[0]
        for database in databases:
            for link in database.links:
                statement = "drop link %s" % link
                execute_statement_on_cbas(client, statement)
                self.log.info("Dropping link %s is %s" % (link, "success"))

    def drop_collections(self, cluster, databases):
        client = cluster.SDKClients[0]
        for database in databases:
            for collection in database.cbas_collections:
                statement = "drop collection %s" % collection
                try:
                    execute_statement_on_cbas(client, statement)
                    self.log.info("Dropping Collection %s is %s" % (collection, "success"))
                except Exception as e:
                    self.log.info("Dropping Collection %s is %s" % (collection, "failed"))

    def disconnect_link(self, cluster, link_name):
        client = cluster.SDKClients[0]
        statement = "DISCONNECT LINK %s" % link_name
        try:
            execute_statement_on_cbas(client, statement)
            self.log.info("Disconnect link %s is %s" % (link_name, "success"))
        except Exception as e:
            self.log.info("Disconnect link %s is %s" % (link_name, "failed"))

    def wait_for_link_disconnect(self, cluster, link_name, timeout=3600):
        st_time = time.time()
        client = random.choice(cluster.SDKClients)
        while time.time() < st_time + timeout:
            time.sleep(10)
            metrics, errors, results = execute_statement_on_cbas(client, "SELECT * FROM Metadata.`Link`")
            if errors is None:
                for result in results:
                    if result["Link"]['Name'] == link_name and \
                        not result["Link"]["IsActive"]:
                        break
                    else:
                        self.log.info("Link state - %s: %s" %(link_name, result["Link"]["IsActive"]))
            else:
                raise Exception("SELECT metadata query failed.")

    def wait_for_link_connect(self, cluster, link_name, timeout=3600):
        st_time = time.time()
        client = random.choice(cluster.SDKClients)
        while time.time() < st_time + timeout:
            time.sleep(10)
            metrics, errors, results = execute_statement_on_cbas(client, "SELECT * FROM Metadata.`Link`")
            if errors is None:
                for result in results:
                    if result["Link"]['Name'] == link_name and \
                        result["Link"]["IsActive"]:
                        break
                    else:
                        self.log.info("Link state - %s: %s" %(link_name, result["Link"]["IsActive"]))
            else:
                raise Exception("SELECT metadata query failed.")

    def discharge_CBAS(self):
        self.stop_run = True

    def wait_for_ingestion(self, cluster, databases, timeout=86400):
        _results = list()
        def check_in_th(collection, items):
            status = False
            stop_time = time.time() + timeout
            while time.time() < stop_time:
                client = random.choice(cluster.SDKClients)
                statement = "select count(*) cnt from {};".format(collection)
                try:
                    if cluster.state == "ACTIVE":
                        metrics, errors, results = execute_statement_on_cbas(client, statement)
                        self.log.debug("dataset: {}, status: {}, actual count: {}, expected count: {}"
                                       .format(collection, "success",
                                               results[0]["cnt"],
                                               items))
                        if results[0]["cnt"] >= items:
                            self.log.info("CBAS dataset is ready: {}".format(collection))
                            status = True
                            break
                except (TimeoutException, AmbiguousTimeoutException, RequestCanceledException,
                        CouchbaseException, Exception) as e:
                    self.log.critical(str(e))
                time.sleep(random.randint(60,120))
            if status is False:
                _results.append(status)
        ths = list()
        for database in databases:
            for collection in database.cbas_collections:
                th = threading.Thread(target=check_in_th,
                                      args=(collection,
                                            database.loadDefn.get("num_items")))
                th.start()
                ths.append(th)
        for th in ths:
            th.join()
        for database in databases:
            self.disconnect_link(cluster, database.link_name)
        return False not in _results


class CBASQueryLoad:
    def __init__(self, cluster, datasource):
        self.bucket = datasource
        self.queries = datasource.cbas_queries
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
        self.cluster = cluster
        self.cluster_conn = None

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
                name="query_thread_{0}".format(self.bucket.name + "_" + str(i)),
                args=(False, 0)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

    def _run_query(self, validate_item_count=False, expected_count=0):
        name = threading.currentThread().getName()
        i = 0
        while not self.stop_run:
            self.cluster_conn = random.choice(self.cluster.SDKClients)
            if self.cluster.state != "ACTIVE":
                time.sleep(60)
                continue
            client_context_id = name + "_" + str(i)
            i += 1
            start = time.time()
            e = ""
            try:
                next(self.total_query_count)
                query_tuple = self.queries[i%len(self.queries)]
                query = query_tuple[0]
                original_query = query_tuple[1]
                # print query
                # print original_query
                metrics, errors, results = execute_statement_on_cbas(
                    self.cluster_conn, query, client_context_id)
                try:
                    self.query_stats[original_query][0] += metrics.execution_time().total_seconds()*1000.0
                    self.query_stats[original_query][1] += 1
                except KeyError:
                    self.query_stats[original_query] = [0, 0]
                    self.query_stats[original_query][0] = metrics.execution_time().total_seconds()*1000.0
                    self.query_stats[original_query][1] = 1
                if errors is None:
                    if validate_item_count:
                        if results[0]['$1'] != expected_count:
                            next(self.failed_count)
                        else:
                            next(self.success_count)
                    else:
                        next(self.success_count)
                else:
                    next(self.failed_count)
            except (TimeoutException, AmbiguousTimeoutException, UnAmbiguousTimeoutException) as ex:
                e = ex
            except RequestCanceledException as ex:
                e = ex
            except InternalServerFailureException as ex:
                e = ex
                next(self.error_count)
            except CouchbaseException as ex:
                e = ex
            except (Exception) as ex:
                e = ex
                self.log.critical(e)
                next(self.error_count)
            if str(e).find("TimeoutException") != -1\
                or str(e).find("AmbiguousTimeoutException") != -1\
                    or str(e).find("UnambiguousTimeoutException") != -1:
                next(self.timeout_count)
            elif str(e).find("RequestCanceledException") != -1:
                self.failures += next(self.cancel_count)
            elif str(e).find("CouchbaseException") != -1:
                self.failures += next(self.error_count)
            if str(e).find("no more information available") != -1:
                self.log.critical(client_context_id + "---->" + query)
                self.log.critical(e)
            end = time.time()
            if end - start < 1:
                time.sleep(end - start)
