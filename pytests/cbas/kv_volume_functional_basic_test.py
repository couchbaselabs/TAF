'''
Created on Apr 23, 2018
@author: riteshagarwal

To run:
/opt/jython/bin/jython -J-cp 'Couchbase-Java-Client-2.5.6/*:jsch-0.1.54.jar:doc_ops.jar' testrunner.py
-i INI_FILE.ini num_query=100,num_items=10000 -t cbas.cursor_drop_test.volume.test_volume,num_query=100,num_items=10000000

'''
import bulk_doc_operations.doc_ops as doc_op
from com.couchbase.client.java.env import DefaultCouchbaseEnvironment
from com.couchbase.client.java import *
from com.couchbase.client.java.transcoder import JsonTranscoder
from com.couchbase.client.java.document import *
from com.couchbase.client.java.document.json import *
from com.couchbase.client.java.query import *
import threading
import random
from java.util.concurrent import Callable
from java.util.concurrent import Executors, TimeUnit
from lib.CbasLib.CBASOperations import CBASHelper
import json
from com.couchbase.client.java.analytics import AnalyticsQuery, AnalyticsParams
import sys, time

from common_lib import sleep
from pytests.cbas.cbas_base import CBASBaseTest
from lib.membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import bucket_utils
from basetestcase import BaseTestCase
from lib.remote.remote_util import RemoteMachineShellConnection
from node_utils.node_ready_functions import NodeHelper


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
                    user = JsonTranscoder().stringToJsonObject(var);
                    doc = JsonDocument.create(str(i+self.start_from), user);
                    docs.append(doc)
                    temp += 1
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkSet(self.bucket, docs)
                        except:
                            sleep(20, "Exception from SDK create. Will retry")
                            try:
                                doc_op().bulkUpsert(self.bucket, docs)
                            except:
                                print "skipping %s documents upload"%len(docs)
                                pass
                        temp = 0
                        docs=[]
#                     response = self.bucket.insert(doc);
                elif self.op_type == "update":
                    var = str(json.dumps(self.generate_GleambookUser(i+self.start_from)))
                    user = JsonTranscoder().stringToJsonObject(var);
                    doc = JsonDocument.create(str(i+self.start_from), user);
                    docs.append(doc)
                    temp += 1
                    if temp == self.batch_size:
                        try:
                            doc_op().bulkUpsert(self.bucket, docs)
                        except:
                            sleep(20, "Exception from SDK create. Will retry")
                            try:
                                doc_op().bulkUpsert(self.bucket, docs)
                            except:
                                print "skipping %s documents upload"%len(docs)
                                pass
                        temp = 0
                        docs=[]

                elif self.op_type == "delete":
                    try:
                        response = self.bucket.remove(str(i+self.start_from));
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

class volume(BaseTestCase):
    def setUp(self, add_defualt_cbas_node=True):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        BaseTestCase.setUp(self)
        self.rest = RestConnection(self.master)
    def tearDown(self):
        pass

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

        self.create_bucket(self.master, "GleambookUsers",bucket_ram=available_memory)
        shell = RemoteMachineShellConnection(self.master)
        command = 'curl -i -u Administrator:password --data \'ns_bucket:update_bucket_props("GleambookUsers", [{extra_config_string, "cursor_dropping_upper_mark=70;cursor_dropping_lower_mark=50"}]).\' http://172.23.104.16:8091/diag/eval'
        shell.execute_command(command)

        result = RestConnection(self.query_node).query_tool("CREATE PRIMARY INDEX idx_GleambookUsers ON GleambookUsers;")
        self.sleep(10, "wait for index creation.")
        self.assertTrue(result['status'] == "success")


    def test_volume(self):
        nodes_in_cluster= [self.servers[0]]
        print "Start Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))

        ########################################################################################################################
        self.log.info("Add a N1QL/Index nodes")
        self.query_node = self.servers[1]
        rest = RestConnection(self.query_node)
        rest.set_data_path(data_path=self.query_node.data_path,index_path=self.query_node.index_path,cbas_path=self.query_node.cbas_path)
        result = self.add_node(self.query_node, rebalance=False)
        self.assertTrue(result, msg="Failed to add N1QL/Index node.")

        self.log.info("Add a KV nodes")
        result = self.add_node(self.servers[2], services=["kv"], rebalance=True)
        self.assertTrue(result, msg="Failed to add KV node.")

        nodes_in_cluster = nodes_in_cluster + [self.servers[1], self.servers[2]]
        ########################################################################################################################
        self.log.info("Step 2: Create Couchbase buckets.")
        self.create_required_buckets()
        for node in nodes_in_cluster:
            NodeHelper.do_a_warm_up(node)
            NodeHelper.wait_service_started(node)
        ########################################################################################################################
        self.log.info("Step 3: Create 10M docs average of 1k docs for 8 couchbase buckets.")
        env = DefaultCouchbaseEnvironment.builder().mutationTokensEnabled(True).computationPoolSize(5).socketConnectTimeout(100000).connectTimeout(100000).maxRequestLifetime(TimeUnit.SECONDS.toMillis(300)).build();
        cluster = CouchbaseCluster.create(env, self.master.ip);
        cluster.authenticate("Administrator","password")
        bucket = cluster.openBucket("GleambookUsers");

        pool = Executors.newFixedThreadPool(5)
        items_start_from = 0
        total_num_items = self.input.param("num_items",5000)

        executors=[]
        num_executors = 5
        doc_executors = 5
        num_items = total_num_items / num_executors
        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items,batch_size=2000))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        updates_from = items_start_from
        deletes_from = items_start_from + total_num_items/10
        items_start_from += total_num_items
        ########################################################################################################################
        self.sleep(120,"Sleeping after 1st cycle.")
        self.log.info("Step 8: Delete 1M docs. Update 1M docs.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 4

        executors.append(GleambookUser_Docloader(bucket, num_items/10, updates_from,"update"))
        executors.append(GleambookUser_Docloader(bucket, num_items/10, deletes_from,"delete"))
        futures = pool.invokeAll(executors)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)

        ########################################################################################################################
        self.sleep(120,"Sleeping after 2nd cycle.")
        pool = Executors.newFixedThreadPool(5)
        num_items = self.input.param("num_items",5000)
        executors=[]
        num_executors = 5
        doc_executors = 5
        num_items = total_num_items / doc_executors

        for i in xrange(doc_executors):
            executors.append(GleambookUser_Docloader(bucket, num_items, items_start_from+i*num_items,batch_size=2000))
        rebalance = self.cluster.async_rebalance(nodes_in_cluster, [self.servers[3]], [])
        futures = pool.invokeAll(executors)

        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        print "Executors completed!!"
        shutdown_and_await_termination(pool, num_executors)
        self.task_manager.get_task_result(rebalance)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
        self.assertTrue(reached, "rebalance failed, stuck or did not complete")

        bucket.close()
        cluster.disconnect()

        print "End Time: %s"%str(time.strftime("%H:%M:%S", time.gmtime(time.time())))
