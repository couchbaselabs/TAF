import json
import threading
import random
import sys
import time

from com.couchbase.client.java.env import DefaultCouchbaseEnvironment
from com.couchbase.client.java import *
from com.couchbase.client.java.transcoder import JsonTranscoder
from com.couchbase.client.java.document import *
from com.couchbase.client.java.document.json import *
from com.couchbase.client.java.query import *
from java.util.concurrent import Callable
from java.util.concurrent import Executors, TimeUnit
from com.couchbase.client.java.analytics import AnalyticsQuery, AnalyticsParams

import bulk_doc_operations.doc_ops as doc_op
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import bucket_utils
from common_lib import sleep
from cbas.cbas_base import CBASBaseTest
from membase.api.rest_client import RestConnection, RestHelper


class global_vars:
    message_id = 1
    start_message_id = 0
    end_message_id = 0


class QueryRunner(Callable):
    def __init__(self, bucket, query, num_queries, cbas_util):
        self.bucket = bucket
        self.num_queries = num_queries
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = None
        self.exception = None
        self.cbas_util = cbas_util
        self.statement = query
        self.params = AnalyticsParams.build()
        self.params = self.params.rawParam("pretty", True)
        self.params = self.params.rawParam("timeout", "120s")
        self.params = self.params.rawParam("username", "Administrator")
        self.params = self.params.rawParam("password", "password")
        self.query = AnalyticsQuery.simple(query, self.params)

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                (self.thread_used, self.num_queries, self.exception,
                 self.completed - self.started, ) #, self.result)
        elif self.completed:
            print "Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                (self.thread_used, self.loaded,
                 self.completed - self.started, ) #, self.result)
        elif self.started:
            return "[%s] %s started at %s" % \
                (self.thread_used, self.num_queries, self.started)
        else:
            return "[%s] %s not yet scheduled" % \
                (self.thread_used, self.num_queries)

    def call(self):
        self.thread_used = threading.currentThread().getName()
        self.started = time.time()
        try:
            self.cbas_util._run_concurrent_queries(self.statement, None, self.num_queries,batch_size=50)
            self.loaded += 1
        except Exception, ex:
            self.exception = ex
        self.completed = time.time()
        return self

class GleambookMessages_Docloader(Callable):
    def __init__(self, msg_bucket, num_items, start_from,op_type="create",batch_size=1000):
        self.msg_bucket = msg_bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = None
        self.exception = None
        self.op_type = op_type
        self.year = range(2001,2018)
        self.month = range(1,12)
        self.day = range(1,28)
        self.batch_size = batch_size

    def generate_GleambookMessages(self, num=None,message_id=None):

        date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
        time = "%02d"%random.choice(range(0,24)) + "-" + "%02d"%random.choice(range(0,60)) + "-" + "%02d"%random.choice(self.day)

        GleambookMessages = {"message_id": "%d"%message_id, "author_id": "%d"%num,
#                              "in_response_to": "%d"%random.choice(range(message_id)),
#                              "sender_location": str(round(random.uniform(0, 100), 4))+","+str(round(random.uniform(0, 100), 4)),
                             "send_time": date+"T"+time,
#                              "message": ''.join(random.choice(string.lowercase) for x in range(50))
                             }
        return GleambookMessages

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                (self.thread_used, self.num_items, self.exception,
                 self.completed - self.started, ) #, self.result)
        elif self.completed:
            print "Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                (self.thread_used, self.loaded,
                 self.completed - self.started, ) #, self.result)
        elif self.started:
            return "[%s] %s started at %s" % \
                (self.thread_used, self.num_items, self.started)
        else:
            return "[%s] %s not yet scheduled" % \
                (self.thread_used, self.num_items)

    def call(self):
        self.thread_used = threading.currentThread().getName()
        self.started = time.time()
        try:
            temp=0
            docs=[]
            for i in xrange(self.num_items):
                start_message_id = global_vars.message_id
                if self.op_type == "create":
                    for j in xrange(random.randint(1,10)):
                        var = str(json.dumps(self.generate_GleambookMessages(i+self.start_from , global_vars.message_id)))
                        user = JsonTranscoder().stringToJsonObject(var)
#                         print i+self.start_from,global_vars.message_id
                        doc = JsonDocument.create(str(global_vars.message_id), user)
                        docs.append(doc)
                        temp+=1
                        if temp == self.batch_size:
                            try:
                                doc_op().bulkSet(self.msg_bucket, docs)
                            except:
                                sleep(20, "Exception in Java SDK - create")
                                try:
                                    doc_op().bulkUpsert(self.msg_bucket, docs)
                                except:
                                    print "skipping %s documents upload"%len(docs)
                                    pass
                            temp = 0
                            docs=[]
                        global_vars.message_id += 1
                    end_message_id = global_vars.message_id
                elif self.op_type == "update":
                    var = str(json.dumps(self.generate_GleambookMessages(i+self.start_from , i+start_message_id)))
                    user = JsonTranscoder().stringToJsonObject(var)
                    doc = JsonDocument.create(str(i+start_message_id), user)
                    docs.append(doc)
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkUpsert(self.msg_bucket, docs)
                        except:
                            sleep(20, "Exception in Java SDK - create")
                            try:
                                doc_op().bulkUpsert(self.msg_bucket, docs)
                            except:
                                print "skipping %s documents upload"%len(docs)
                                pass
                        temp = 0
                        docs=[]
                elif self.op_type == "delete":
                    try:
                        response = self.msg_bucket.remove(str(i+start_message_id))
                    except:
                        pass
                self.loaded += 1
        except Exception, ex:
            import traceback
            traceback.print_exc()
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            self.exception = ex
        self.completed = time.time()
        return self

class GleambookUser_Docloader(Callable):
    def __init__(self, bucket, num_items, start_from,op_type="create", batch_size=2000):
        self.bucket = bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = None
        self.exception = None
        self.op_type = op_type
        self.year = range(2001,2018)
        self.month = range(1,12)
        self.day = range(1,28)
        self.hr = range(0,24)
        self.min = range(0,60)
        self.sec = range(0,60)
        self.batch_size = batch_size

    def generate_GleambookUser(self, num=None):
        organization = ["Wipro","Infosys","TCS","Tech Mahindra","CTS","Microsoft"]
        date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
        time = "%02d"%random.choice(self.hr) + "-" + "%02d"%random.choice(self.min) + "-" + "%02d"%random.choice(self.sec)
        employment = []
        start_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
        end_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)

        for i in xrange(3):
#             start_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)
#             end_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)

            EmploymentType = {"organization":random.choice(organization),"start_date":start_date,"end_date":end_date}
            employment.append(EmploymentType)

#             start_date = "%04d"%random.choice(self.year) + "-" + "%02d"%random.choice(self.month) + "-" + "%02d"%random.choice(self.day)

            EmploymentType = {"organization":random.choice(organization),"start_date":start_date}
            employment.append(EmploymentType)

        GleambookUserType = {"id":num,"alias":"Peter"+"%05d"%num,"name":"Peter Thomas","user_since":date+"T"+time,
#                              "friend_ids":random.sample(range(1000),random.choice(range(10))),
                            "employment":random.sample(employment,random.choice(range(6)))
                             }
        return GleambookUserType

    def __str__(self):
        if self.exception:
            return "[%s] %s download error %s in %.2fs" % \
                (self.thread_used, self.num_items, self.exception,
                 self.completed - self.started, ) #, self.result)
        elif self.completed:
            print "Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
            return "[%s] %s items loaded in %.2fs" % \
                (self.thread_used, self.loaded,
                 self.completed - self.started, ) #, self.result)
        elif self.started:
            return "[%s] %s started at %s" % \
                (self.thread_used, self.num_items, self.started)
        else:
            return "[%s] %s not yet scheduled" % \
                (self.thread_used, self.num_items)

    def call(self):
        self.thread_used = threading.currentThread().getName()
        self.started = time.time()
        try:
            docs=[]
            temp=0
            for i in xrange(self.num_items):
                if self.op_type == "create":
                    var = str(json.dumps(self.generate_GleambookUser(i+self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var)
                    doc = JsonDocument.create(str(i+self.start_from), user)
                    docs.append(doc)
                    temp += 1
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkSet(self.bucket, docs)
                        except:
                            sleep(20, "Exception in Java SDK - create")
                            try:
                                doc_op().bulkUpsert(self.bucket, docs)
                            except:
                                print "skipping %s documents upload"%len(docs)
                                pass
                        temp = 0
                        docs=[]
#                     response = self.bucket.insert(doc)
                elif self.op_type == "update":
                    var = str(json.dumps(self.generate_GleambookUser(i+self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var)
                    doc = JsonDocument.create(str(i+self.start_from), user)
                    docs.append(doc)
                    temp += 1
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkUpsert(self.bucket, docs)
                        except:
                            sleep(20, "Exception in Java SDK - create")
                            try:
                                doc_op().bulkUpsert(self.bucket, docs)
                            except:
                                print "skipping %s documents upload"%len(docs)
                                pass
                        temp = 0
                        docs=[]

                elif self.op_type == "delete":
                    try:
                        response = self.bucket.remove(str(i+self.start_from))
                    except:
                        print "Exception from Java SDK - remove"
                self.loaded += 1
        except Exception, ex:
            import traceback
            traceback.print_exc()
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            self.exception = ex
        self.completed = time.time()
        return self

def shutdown_and_await_termination(pool, timeout):
    pool.shutdown()
    try:
        if not pool.awaitTermination(timeout, TimeUnit.SECONDS):
            pool.shutdownNow()
            if (not pool.awaitTermination(timeout, TimeUnit.SECONDS)):
                print >> sys.stderr, "Pool did not terminate"
    except InterruptedException, ex:
        # (Re-)Cancel if current thread also interrupted
        pool.shutdownNow()
        # Preserve interrupt status
        Thread.currentThread().interrupt()

class analytics(CBASBaseTest):
    def setUp(self, add_defualt_cbas_node=True):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        CBASBaseTest.setUp(self, add_defualt_cbas_node=add_defualt_cbas_node)

    def create_required_buckets(self):
        self.log.info("Get the available memory quota")
        bucket_util = bucket_utils(self.master)
        self.info = bucket_util.rest.get_nodes_self()
        threadhold_memory = 1024
        total_memory_in_mb = self.info.memoryFree / 1024 ** 2
        total_available_memory_in_mb = total_memory_in_mb
        active_service = self.info.services

        if "index" in active_service:
            total_available_memory_in_mb -= self.info.indexMemoryQuota
        if "fts" in active_service:
            total_available_memory_in_mb -= self.info.ftsMemoryQuota
        if "cbas" in active_service:
            total_available_memory_in_mb -= self.info.cbasMemoryQuota
        if "eventing" in active_service:
            total_available_memory_in_mb -= self.info.eventingMemoryQuota

        print(total_memory_in_mb)
        available_memory =  total_available_memory_in_mb - threadhold_memory
        self.rest.set_service_memoryQuota(service='memoryQuota', memoryQuota=available_memory)
        self.rest.set_service_memoryQuota(service='cbasMemoryQuota', memoryQuota=available_memory-1024)
        self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=available_memory-1024)

        self.log.info("Create CB buckets")
        self.create_bucket(self.master, "ChirpMessages", bucket_ram=100)
        available_memory -= 100
        self.create_bucket(self.master, "GleambookUsers", bucket_ram=int(available_memory*1/3))
        self.create_bucket(self.master, "GleambookMessages", bucket_ram=int(available_memory*2/3))


        result = RestConnection(self.query_node).query_tool("CREATE PRIMARY INDEX idx_GleambookUsers ON GleambookUsers;")
        self.sleep(10, "wait for index creation.")
        self.assertTrue(result['status'] == "success")

        result = RestConnection(self.query_node).query_tool("CREATE PRIMARY INDEX idx_GleambookMessages ON GleambookMessages;")
        self.sleep(10, "wait for index creation.")
        self.assertTrue(result['status'] == "success")

        result = RestConnection(self.query_node).query_tool("CREATE PRIMARY INDEX idx_ChirpMessages ON ChirpMessages;")
        self.sleep(10, "wait for index creation.")
        self.assertTrue(result['status'] == "success")

    def setup_cbas(self):
        self.cbas_util.createConn("GleambookUsers")
        self.cleanup_cbas()

        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name="GleambookUsers",
                                                cbas_dataset_name="GleambookUsers_ds")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name="GleambookMessages",
                                                cbas_dataset_name="GleambookMessages_ds")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name="ChirpMessages",
                                                cbas_dataset_name="ChirpMessages_ds")

        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name="GleambookUsers",
                                                cbas_dataset_name="GleambookUsers_ds1")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name="GleambookMessages",
                                                cbas_dataset_name="GleambookMessages_ds1")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name="ChirpMessages",
                                                cbas_dataset_name="ChirpMessages_ds1")

        # Connect to Bucket
        self.connect_cbas_buckets()

    def connect_cbas_buckets(self):
        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name='GleambookUsers',
                               cb_bucket_password=self.cb_bucket_password)
        self.cbas_util.connect_to_bucket(cbas_bucket_name='GleambookMessages',
                               cb_bucket_password=self.cb_bucket_password)
        self.cbas_util.connect_to_bucket(cbas_bucket_name='ChirpMessages',
                               cb_bucket_password=self.cb_bucket_password)

    def disconnect_cbas_buckets(self):

        result = self.cbas_util.disconnect_from_bucket(cbas_bucket_name="GleambookUsers")
        self.assertTrue(result, "Disconnect GleambookUsers bucket failed")
        result = self.cbas_util.disconnect_from_bucket(cbas_bucket_name="GleambookMessages")
        self.assertTrue(result, "Disconnect GleambookMessages bucket failed")
        result = self.cbas_util.disconnect_from_bucket(cbas_bucket_name="ChirpMessages")
        self.assertTrue(result, "Disconnect ChirpMessages bucket failed")

    def create_cbas_indexes(self):
        #create Indexes
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
            "CREATE INDEX usrSinceIdx ON `GleambookUsers_ds`(user_since: string);",timeout=300,analytics_timeout=300)
        self.assertTrue(status == "success", "Create Index query failed")
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
            "CREATE INDEX authorIdIdx ON `GleambookMessages_ds`(author_id: string);",timeout=300,analytics_timeout=300)
        self.assertTrue(status == "success", "Create Index query failed")
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
            "CREATE INDEX sndTimeIdx  ON `ChirpMessages_ds`(send_time: string);",timeout=300,analytics_timeout=300)
        self.assertTrue(status == "success", "Create Index query failed")

        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
            "CREATE INDEX usrSinceIdx1 ON `GleambookUsers_ds1`(user_since: string);",timeout=300,analytics_timeout=300)
        self.assertTrue(status == "success", "Create Index query failed")
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
            "CREATE INDEX authorIdIdx1 ON `GleambookMessages_ds1`(author_id: string);",timeout=300,analytics_timeout=300)
        self.assertTrue(status == "success", "Create Index query failed")
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
            "CREATE INDEX sndTimeIdx1  ON `ChirpMessages_ds1`(send_time: string);",timeout=300,analytics_timeout=300)
        self.assertTrue(status == "success", "Create Index query failed")

    def validate_items_count(self):
        items_GleambookUsers = RestConnection(self.query_node).query_tool('select count(*) from GleambookUsers')['results'][0]['$1']
        items_GleambookMessages = RestConnection(self.query_node).query_tool('select count(*) from GleambookMessages')['results'][0]['$1']
        items_ChirpMessages = RestConnection(self.query_node).query_tool('select count(*) from ChirpMessages')['results'][0]['$1']
        self.log.info("Items in CB GleanBookUsers bucket: %s"%items_GleambookUsers)
        self.log.info("Items in CB GleambookMessages bucket: %s"%items_GleambookMessages)
        self.log.info("Items in CB ChirpMessages bucket: %s"%items_ChirpMessages)

        self.sleep(60)
        result = False
        tries = 10
        while tries>0:
            try:
                result = self.cbas_util.validate_cbas_dataset_items_count("GleambookUsers_ds",items_GleambookUsers, num_tries=100)
                break
            except:
                pass
            tries -= 1
        self.assertTrue(result,"No. of items in GleambookUsers dataset do not match that in the CB bucket")

        result = False
        tries = 10
        while tries>0:
            try:
                result = self.cbas_util.validate_cbas_dataset_items_count("GleambookMessages_ds",items_GleambookMessages, num_tries=100)
                break
            except:
                pass
            tries -= 1
        self.assertTrue(result,"No. of items in GleambookMessages dataset do not match that in the CB bucket")

        result = False
        tries = 10
        while tries>0:
            try:
                result = self.cbas_util.validate_cbas_dataset_items_count("ChirpMessages_ds",items_ChirpMessages, num_tries=100)
                break
            except:
                pass
            tries -= 1
        self.assertTrue(result,"No. of items in ChirpMessages dataset do not match that in the CB bucket")

        result = False
        tries = 10
        while tries>0:
            try:
                result = self.cbas_util.validate_cbas_dataset_items_count("GleambookUsers_ds1",items_GleambookUsers, num_tries=100)
                break
            except:
                pass
            tries -= 1
        self.assertTrue(result,"No. of items in GleambookUsers dataset do not match that in the CB bucket")

        result = False
        tries = 10
        while tries>0:
            try:
                result = self.cbas_util.validate_cbas_dataset_items_count("GleambookMessages_ds1",items_GleambookMessages, num_tries=100)
                break
            except:
                pass
            tries -= 1
        self.assertTrue(result,"No. of items in GleambookMessages dataset do not match that in the CB bucket")

        result = False
        tries = 10
        while tries>0:
            try:
                result = self.cbas_util.validate_cbas_dataset_items_count("ChirpMessages_ds1",items_ChirpMessages, num_tries=100)
                break
            except:
                pass
            tries -= 1
        self.assertTrue(result,"No. of items in ChirpMessages dataset do not match that in the CB bucket")

    def test_analytics_volume(self):
        queries = ['SELECT VALUE u FROM `GleambookUsers_ds` u WHERE u.user_since >= "2010-02-13T16-48-15" AND u.user_since < "2010-10-13T16-48-15" AND (SOME e IN u.employment SATISFIES e.end_date IS UNKNOWN) LIMIT 100;',
           'SELECT VALUE u FROM `GleambookUsers_ds` u WHERE u.user_since >= "2010-02-13T16-48-15" AND u.user_since < "2010-12-13T16-48-15" limit 100;',
           'SELECT META(u).id AS id, COUNT(*) AS count FROM `GleambookUsers_ds` u, `GleambookMessages_ds` m WHERE TO_STRING(META(u).id) = m.author_id \
           AND u.user_since >= "2010-02-13T16-48-15" AND u.user_since < "2010-03-13T16-48-15" AND m.send_time >= "2011-02-01T12-23-09" AND \
           m.send_time < "2011-03-01T12-23-09" GROUP BY META(u).id;',
           'SELECT META(u).id AS id, COUNT(*) AS count FROM `GleambookUsers_ds` u, `GleambookMessages_ds` m WHERE TO_STRING(META(u).id) = m.author_id \
           AND u.user_since >= "2010-02-13T16-48-15" AND u.user_since < "2010-03-13T16-48-15" AND m.send_time >= "2011-10-01T12-23-09" \
           AND m.send_time < "2011-08-01T12-23-09" GROUP BY META(u).id ORDER BY count LIMIT 10;'
           ]
        nodes_in_cluster= [self.servers[0],self.cbas_node]
        print "Start Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))

        ########################################################################################################################
        self.log.info("Step 1: Start the test with 2 KV and 2 CBAS nodes")

        self.log.info("Add a N1QL/Index nodes")
        self.query_node = self.servers[1]
        rest = RestConnection(self.query_node)
        rest.set_data_path(data_path=self.query_node.data_path,index_path=self.query_node.index_path,cbas_path=self.query_node.cbas_path)
        result = self.add_node(self.query_node, rebalance=False)
        self.assertTrue(result, msg="Failed to add N1QL/Index node.")
        self.log.info("Add a KV nodes")
        rest = RestConnection(self.cluster.kv_nodes[1])
        rest.set_data_path(data_path=self.cluster.kv_nodes[1].data_path,index_path=self.cluster.kv_nodes[1].index_path,cbas_path=self.cluster.kv_nodes[1].cbas_path)
        result = self.add_node(self.cluster.kv_nodes[1], services=["kv"], rebalance=False)
        self.assertTrue(result, msg="Failed to add KV node.")

        self.log.info("Add one more KV node")
        rest = RestConnection(self.cluster.kv_nodes[3])
        rest.set_data_path(data_path=self.cluster.kv_nodes[3].data_path,index_path=self.cluster.kv_nodes[3].index_path,cbas_path=self.cluster.kv_nodes[3].cbas_path)
        result = self.add_node(self.cluster.kv_nodes[3], services=["kv"], rebalance=False)
        self.assertTrue(result, msg="Failed to add KV node.")

        self.log.info("Add one more KV node")
        rest = RestConnection(self.cluster.kv_nodes[4])
        rest.set_data_path(data_path=self.cluster.kv_nodes[4].data_path,index_path=self.cluster.kv_nodes[4].index_path,cbas_path=self.cluster.kv_nodes[4].cbas_path)
        result = self.add_node(self.cluster.kv_nodes[4], services=["kv"], rebalance=False)
        self.assertTrue(result, msg="Failed to add KV node.")

        self.log.info("Add a CBAS nodes")
        result = self.add_node(self.cluster.cbas_nodes[0], services=["cbas"], rebalance=True)
        self.assertTrue(result, msg="Failed to add CBAS node.")

        nodes_in_cluster = nodes_in_cluster + [self.query_node, self.cluster.kv_nodes[1], self.cluster.kv_nodes[3], self.cluster.kv_nodes[4], self.cluster.cbas_nodes[0]]
        ########################################################################################################################
        self.log.info("Step 2: Create Couchbase buckets.")
        self.create_required_buckets()

        ########################################################################################################################
        self.log.info("Step 3: Create 10M docs average of 1k docs for 8 couchbase buckets.")
        env = DefaultCouchbaseEnvironment.builder().mutationTokensEnabled(True).computationPoolSize(5).socketConnectTimeout(100000).connectTimeout(100000).maxRequestLifetime(TimeUnit.SECONDS.toMillis(300)).build()
        cluster = CouchbaseCluster.create(env, self.master.ip)
        cluster.authenticate("Administrator","password")
        bucket = cluster.openBucket("GleambookUsers")
        msg_bucket = cluster.openBucket("GleambookMessages")

        pool = Executors.newFixedThreadPool(5)
        items_start_from = 0
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        doc_executors = 5
        num_items = total_num_items / num_executors
        query_executors = num_executors - doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items
        ########################################################################################################################
        self.log.info("Step 4: Create 8 analytics buckets and 8 datasets and connect.")
        self.setup_cbas()

        ########################################################################################################################
        self.log.info("Step 5: Wait for ingestion to complete.")
        self.sleep(10,"Wait for the ingestion to complete")

        ########################################################################################################################
        self.log.info("Step 6: Verify the items count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 7: Disconnect CBAS bucket and create secondary indexes.")
        self.disconnect_cbas_buckets()
        self.create_cbas_indexes()

        ########################################################################################################################
        self.log.info("Step 8: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = num_executors - doc_executors

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
        ########################################################################################################################
        self.log.info("Step 9: Connect cbas buckets.")
        self.connect_cbas_buckets()
        self.sleep(10,"Wait for the ingestion to complete")

        ########################################################################################################################
        self.log.info("Step 10: Verify the items count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 12: When 11 is in progress do a KV Rebalance in of 1 nodes.")
        rest = RestConnection(self.cluster.kv_nodes[2])
        rest.set_data_path(data_path=self.cluster.kv_nodes[2].data_path,index_path=self.cluster.kv_nodes[2].index_path,cbas_path=self.cluster.kv_nodes[2].cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [self.cluster.kv_nodes[2]], [])
        nodes_in_cluster += [self.cluster.kv_nodes[2]]
        ########################################################################################################################
        self.log.info("Step 11: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items

        ########################################################################################################################
        self.log.info("Step 13: Wait for rebalance to complete.")
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)

        ########################################################################################################################
        self.log.info("Step 14: Verify the items count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 15: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = num_executors - doc_executors

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))

        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
        ########################################################################################################################
        self.log.info("Step 16: Verify Results that 1M docs gets deleted from analytics datasets.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 17: Disconnect CBAS buckets.")
        self.disconnect_cbas_buckets()

        ########################################################################################################################
        self.log.info("Step 18: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        doc_executors = 4
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items

        ########################################################################################################################
        self.log.info("Step 19: Multiple Connect/Disconnect CBAS buckets during ingestion in step 18.")
        self.connect_cbas_buckets()
        self.sleep(5)
        self.disconnect_cbas_buckets()
        self.connect_cbas_buckets()
        self.sleep(5)
        self.disconnect_cbas_buckets()
        self.connect_cbas_buckets()
        self.sleep(5)
        self.disconnect_cbas_buckets()
        self.connect_cbas_buckets()
        ########################################################################################################################
        self.log.info("Step 20: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 21: Run 500 complex queries concurrently and verify the results.")
        pool = Executors.newFixedThreadPool(5)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        query_executors = num_executors
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))

        self.log.info("Step 22: When 21 is in progress do a KV Rebalance out of 2 nodes.")
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [], self.cluster.kv_nodes[1:3])
        nodes_in_cluster = [node for node in nodes_in_cluster if node not in self.cluster.kv_nodes[1:3]]

        futures = pool.invokeAll(executors)
        self.log.info("Step 23: Wait for rebalance.")
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)

        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 24: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 6
        doc_executors = 4
        query_executors = 1
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))

        self.log.info("Step 26: Run 500 complex queries concurrently and verify the results.")
        executors.append(QueryRunner(bucket,random.choice(queries),500,self.cbas_util))


        ##################################################### NEED TO BE UPDATED ##################################################################
        self.log.info("Step 25: When 24 is in progress do a CBAS Rebalance in of 2 nodes.")
        for node in self.cluster.cbas_nodes[2:]:
            rest = RestConnection(node)
            rest.set_data_path(data_path=node.data_path,index_path=node.index_path,cbas_path=node.cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, self.cluster.cbas_nodes[1:],[],services=["cbas","cbas"])
        nodes_in_cluster = nodes_in_cluster + self.cluster.cbas_nodes[1:]
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items

        self.log.info("Step 27: Wait for rebalance to complete.")
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

        ########################################################################################################################
        self.log.info("Step 28: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 29: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = num_executors - doc_executors

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))

        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
        ########################################################################################################################
        self.log.info("Step 30: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 31: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = 1
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))


        ###################################################### NEED TO BE UPDATED ##################################################################
        self.log.info("Step 32: When 31 is in progress do a CBAS Rebalance out of 2 nodes.")
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [], self.cluster.cbas_nodes[1:])
        nodes_in_cluster = [node for node in nodes_in_cluster if node not in self.cluster.cbas_nodes[1:]]
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items
        ########################################################################################################################
        self.log.info("Step 33: Wait for rebalance to complete.")
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)

        ########################################################################################################################
        self.log.info("Step 34: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 35: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = num_executors - doc_executors

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))

        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 36: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 37: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = 1
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))

        ###################################################### NEED TO BE UPDATED ##################################################################
        self.log.info("Step 38: When 37 is in progress do a CBAS SWAP Rebalance of 2 nodes.")
        for node in self.cluster.cbas_nodes[-1:]:
            rest = RestConnection(node)
            rest.set_data_path(data_path=node.data_path,index_path=node.index_path,cbas_path=node.cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster,self.cluster.cbas_nodes[-1:], [self.cbas_node],services=["cbas"],check_vbucket_shuffling=False)
        nodes_in_cluster += self.cluster.cbas_nodes[-1:]
        nodes_in_cluster.remove(self.cbas_node)
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 39: Wait for rebalance to complete.")
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items

        ########################################################################################################################
        self.log.info("Step 40: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 41: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = num_executors - doc_executors

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))

        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 42: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 43: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = 1
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))

        ###################################################### NEED TO BE UPDATED ##################################################################
        self.log.info("Step 44: When 43 is in progress do a KV+CBAS Rebalance IN.")
        rest = RestConnection(self.cbas_node)
        rest.set_data_path(data_path=self.cbas_node.data_path,index_path=self.cbas_node.index_path,cbas_path=self.cbas_node.cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [self.cbas_node], [],services=["cbas"])
        nodes_in_cluster += [self.cbas_node]
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        rest = RestConnection(self.cluster.kv_nodes[1])
        rest.set_data_path(data_path=self.cluster.kv_nodes[1].data_path,index_path=self.cluster.kv_nodes[1].index_path,cbas_path=self.cluster.kv_nodes[1].cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [self.cluster.kv_nodes[1]], [])
        nodes_in_cluster += [self.cluster.kv_nodes[1]]
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 45: Wait for rebalance to complete.")
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items

        ########################################################################################################################
        self.log.info("Step 46: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 47: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = num_executors - doc_executors

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))

        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 48: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 49: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = 1
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))

        ########################################################################################################################
        self.log.info("Step 50: When 49 is in progress do a KV+CBAS Rebalance OUT.")
        rest = RestConnection(self.cluster.kv_nodes[2])
        rest.set_data_path(data_path=self.cluster.kv_nodes[2].data_path,index_path=self.cluster.kv_nodes[2].index_path,cbas_path=self.cluster.kv_nodes[2].cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [self.cluster.kv_nodes[2]], self.cluster.cbas_nodes[-1:]+[self.cluster.kv_nodes[1]])
#         self.task_manager.get_task_result(rebalance)
        nodes_in_cluster = [node for node in nodes_in_cluster if node not in self.cluster.cbas_nodes[-1:]] + [self.cluster.kv_nodes[2]]
        nodes_in_cluster.remove(self.cluster.kv_nodes[1])

        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 51: Wait for rebalance to complete.")
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)
        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items

        ########################################################################################################################
        self.log.info("Step 52: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 53: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = num_executors - doc_executors

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))

        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 54: Verify the docs count.")
        self.validate_items_count()


        ########################################################################################################################
        self.log.info("Step 55: Create 10M docs.")
        pool = Executors.newFixedThreadPool(5)
        total_num_items = self.input.param("num_items",5000)
        num_query = self.input.param("num_query",500)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = 1
        num_items = total_num_items / doc_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items))
            executors.append(GleambookMessages_Docloader(msg_bucket, num_items, items_start_from+i*num_items))
        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))

        ########################################################################################################################
        self.log.info("Step 56: When 55 is in progress do a KV+CBAS SWAP Rebalance .")
        for node in self.cluster.cbas_nodes[-1:]:
            rest = RestConnection(node)
            rest.set_data_path(data_path=node.data_path,index_path=node.index_path,cbas_path=node.cbas_path)
        rest = RestConnection(self.cluster.kv_nodes[1])
        rest.set_data_path(data_path=self.cluster.kv_nodes[1].data_path,index_path=self.cluster.kv_nodes[1].index_path,cbas_path=self.cluster.kv_nodes[1].cbas_path)
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, self.cluster.cbas_nodes[-1:]+[self.cluster.kv_nodes[1]], [self.cbas_node, self.cluster.kv_nodes[2]],services=["cbas","kv"])
#         self.task_manager.get_task_result(rebalance)
        nodes_in_cluster.remove(self.cbas_node)
        nodes_in_cluster.remove(self.cluster.kv_nodes[2])
        nodes_in_cluster = [node for node in nodes_in_cluster if node not in self.cluster.cbas_nodes[-1:]]
        nodes_in_cluster += [self.cluster.kv_nodes[1]]

        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 57: Wait for rebalance to complete.")
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=240)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")
        self.sleep(20)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items

        ########################################################################################################################
        self.log.info("Step 58: Verify the docs count.")
        self.validate_items_count()

        ########################################################################################################################
        self.log.info("Step 59: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4
        query_executors = num_executors - doc_executors

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookMessages_Docloader(msg_bucket, num_items/10, deletes_from,"delete"))

        for i in xrange(query_executors):
            executors.append(QueryRunner(bucket,random.choice(queries),num_query,self.cbas_util))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.log.info("Step 60: Verify the docs count.")
        self.validate_items_count()


        bucket.close()
        msg_bucket.close()
        cluster.disconnect()

        print "End Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))

