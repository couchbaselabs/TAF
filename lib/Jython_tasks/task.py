'''
Created on Sep 14, 2017

@author: riteshagarwal
'''
from com.couchbase.client.java import *;
from com.couchbase.client.java.transcoder import JsonTranscoder
from com.couchbase.client.java.document import *;
from com.couchbase.client.java.document.json import *;
from com.couchbase.client.java.query import *;
import threading
from threading import Thread
import string
import random
import sys
import time
import logging
from java.util.concurrent import Callable
from java.util.concurrent import Executors, TimeUnit
import json
from Jython_tasks.shutdown import shutdown_and_await_termination
from membase.api.rest_client import RestConnection
from BucketLib.BucketOperations import BucketHelper
from membase.api.exception import N1QLQueryException, DropIndexException, CreateIndexException, DesignDocCreationException, QueryViewException, ReadDocumentException, RebalanceFailedException, \
                                    GetBucketInfoFailed, CompactViewFailed, SetViewInfoNotFound, FailoverFailedException, \
                                    ServerUnavailableException
from httplib import IncompleteRead                                   


log = logging.getLogger(__name__)

class DocloaderTask(Callable):
    def __init__(self, bucket, num_items, start_from, k, v, thread_name):
        self.bucket = bucket
        self.num_items = num_items
        self.start_from = start_from
        self.started = None
        self.completed = None
        self.loaded = 0
        self.thread_used = thread_name
        self.exception = None
        self.key = k
        self.value = v

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
        self.started = time.time()
        try:
            var = str(json.dumps(self.value))
            user = JsonTranscoder().stringToJsonObject(var);
#             user = JsonObject.fromJson(str(self.value))
            for i in xrange(self.num_items):
                doc = JsonDocument.create(self.key+str(i+self.start_from), user);
                response = self.bucket.upsert(doc);
                self.loaded += 1
        except Exception, ex:
            self.exception = ex
        self.completed = time.time()
        return self
    
class docloadertask_executor():
    
    def load(self, k, v, docs=10000, server="localhost", bucket="default"):
        cluster = CouchbaseCluster.create(server);
        cluster.authenticate("Administrator","password")
        bucket = cluster.openBucket(bucket);
        
        pool = Executors.newFixedThreadPool(5)
        docloaders=[]
        num_executors = 5
        total_num_executors = 5
        num_docs = docs/total_num_executors
        for i in xrange(total_num_executors):
            docloaders.append(DocloaderTask(bucket, num_docs, i*num_docs, k, v))
        futures = pool.invokeAll(docloaders)
        for future in futures:
            print future.get(num_executors, TimeUnit.SECONDS)
        
        print "Executors completed!!"
        shutdown_and_await_termination(pool, 5)
        if bucket.close() and cluster.disconnect():
            pass
        
class rebalanceTask(Callable):
    
    def __init__(self, servers, to_add=[], to_remove=[], do_stop=False, progress=30,
                 use_hostnames=False, services = None, check_vbucket_shuffling=True):
        self.servers = servers
        self.to_add = to_add
        self.to_remove = to_remove
        self.start_time = None
        self.services = services
        self.monitor_vbuckets_shuffling = False
        self.check_vbucket_shuffling = check_vbucket_shuffling
        try:
            self.rest = RestConnection(self.servers[0])
        except ServerUnavailableException, e:
            log.error(e)
            raise e
        self.retry_get_progress = 0
        self.use_hostnames = use_hostnames
        self.previous_progress = 0
        self.old_vbuckets = {}
        self.thread_used = "Rebalance_task"
    
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
        self.started = time.time()
        try:
            if len(self.to_add) and len(self.to_add) == len(self.to_remove):
                node_version_check = self.rest.check_node_versions()
                non_swap_servers = set(self.servers) - set(self.to_remove) - set(self.to_add)
                if self.check_vbucket_shuffling:
                    self.old_vbuckets = BucketHelper(self.servers[0])._get_vbuckets(non_swap_servers, None)
                if self.old_vbuckets and self.check_vbucket_shuffling:
                    self.monitor_vbuckets_shuffling = True
                if self.monitor_vbuckets_shuffling and node_version_check and self.services:
                    for service_group in self.services:
                        if "kv" not in service_group:
                            self.monitor_vbuckets_shuffling = False
                if self.monitor_vbuckets_shuffling and node_version_check:
                    services_map = self.rest.get_nodes_services()
                    for remove_node in self.to_remove:
                         key = "{0}:{1}".format(remove_node.ip,remove_node.port)
                         services = services_map[key]
                         if "kv" not in services:
                            self.monitor_vbuckets_shuffling = False
                if self.monitor_vbuckets_shuffling:
                    log.info("This is swap rebalance and we will monitor vbuckets shuffling")
            self.add_nodes()
            self.start_rebalance()
            self.check()
            #self.task_manager.schedule(self)
        except Exception as e:
            self.exception = e
            self.result = False
            log.info(str(e))
            return self.result
        self.completed = time.time()
        self.result = True
        return self.result
    
    def add_nodes(self):
        master = self.servers[0]
        services_for_node = None
        node_index = 0
        for node in self.to_add:
            log.info("adding node {0}:{1} to cluster".format(node.ip, node.port))
            if self.services != None:
                services_for_node = [self.services[node_index]]
                node_index += 1
            if self.use_hostnames:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.hostname, node.port, services = services_for_node)
            else:
                self.rest.add_node(master.rest_username, master.rest_password,
                                   node.ip, node.port, services = services_for_node)

    def start_rebalance(self):
        nodes = self.rest.node_statuses()

        # Determine whether its a cluster_run/not
        cluster_run = True

        firstIp = self.servers[0].ip
        if len(self.servers) == 1 and self.servers[0].port == '8091':
            cluster_run = False
        else:
            for node in self.servers:
                if node.ip != firstIp:
                    cluster_run = False
                    break
        ejectedNodes = []

        for server in self.to_remove:
            for node in nodes:
                if cluster_run:
                    if int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
                        log.info("removing node {0}:{1} to cluster".format(node.ip, node.port))
                else:
                    if self.use_hostnames:
                        if server.hostname == node.ip and int(server.port) == int(node.port):
                            ejectedNodes.append(node.id)
                            log.info("removing node {0}:{1} to cluster".format(node.ip, node.port))
                    elif server.ip == node.ip and int(server.port) == int(node.port):
                        ejectedNodes.append(node.id)
                        log.info("removing node {0}:{1} to cluster".format(node.ip, node.port))
                
        if self.rest.is_cluster_mixed():
            # workaround MB-8094
            log.warn("cluster is mixed. sleep for 15 seconds before rebalance")
            time.sleep(15)

        self.rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)
        self.start_time = time.time()

    def check(self):
        status = None
        progress = -100
        try:
            if self.monitor_vbuckets_shuffling:
                log.info("This is swap rebalance and we will monitor vbuckets shuffling")
                non_swap_servers = set(self.servers) - set(self.to_remove) - set(self.to_add)
                new_vbuckets = BucketHelper(self.servers[0])._get_vbuckets(non_swap_servers, None)
                for vb_type in ["active_vb", "replica_vb"]:
                    for srv in non_swap_servers:
                        if not(len(self.old_vbuckets[srv][vb_type]) + 1 >= len(new_vbuckets[srv][vb_type]) and\
                           len(self.old_vbuckets[srv][vb_type]) - 1 <= len(new_vbuckets[srv][vb_type])):
                            msg = "Vbuckets were suffled! Expected %s for %s" % (vb_type, srv.ip) + \
                                " are %s. And now are %s" % (
                                len(self.old_vbuckets[srv][vb_type]),
                                len(new_vbuckets[srv][vb_type]))
                            log.error(msg)
                            log.error("Old vbuckets: %s, new vbuckets %s" % (self.old_vbuckets, new_vbuckets))
                            raise Exception(msg)
            (status, progress) = self.rest._rebalance_status_and_progress()
            log.info("Rebalance - status: %s, progress: %s", status, progress)
            # if ServerUnavailableException
            if progress == -100:
                self.retry_get_progress += 1
            if self.previous_progress != progress:
                self.previous_progress = progress
                self.retry_get_progress = 0
            else:
                self.retry_get_progress += 1
        except RebalanceFailedException as ex:
            self.result = False
            raise ex
        # catch and set all unexpected exceptions
        except Exception as e:
            self.result = False
            raise e
        retry_get_process_num = 25
        if self.rest.is_cluster_mixed():
            """ for mix cluster, rebalance takes longer """
            log.info("rebalance in mix cluster")
            retry_get_process_num = 40
        # we need to wait for status to be 'none' (i.e. rebalance actually finished and
        # not just 'running' and at 100%) before we declare ourselves done
        if progress != -1 and status != 'none':
            if self.retry_get_progress < retry_get_process_num:
                time.sleep(3)
                self.check()
            else:
                self.result = False
                self.rest.print_UI_logs()
                raise RebalanceFailedException("seems like rebalance hangs. please check logs!")
        else:
            success_cleaned = []
            for removed in self.to_remove:
                try:
                    rest = RestConnection(removed)
                except ServerUnavailableException, e:
                    log.error(e)
                    continue
                start = time.time()
                while time.time() - start < 30:
                    try:
                        if 'pools' in rest.get_pools_info() and \
                                      (len(rest.get_pools_info()["pools"]) == 0):
                            success_cleaned.append(removed)
                            break
                        else:
                            time.sleep(0.1)
                    except (ServerUnavailableException, IncompleteRead), e:
                        log.error(e)
            result = True
            for node in set(self.to_remove) - set(success_cleaned):
                log.error("node {0}:{1} was not cleaned after removing from cluster"\
                                                              .format(node.ip, node.port))
                self.result = False

            log.info("rebalancing was completed with progress: {0}% in {1} sec".
                          format(progress, time.time() - self.start_time))
            self.result = True
            return 
    
class TestTask(Callable):
    def __init__(self, timeout):
        self.thread_used = "test_task"
        self.started = None
        self.completed = None
        self.exception = None
        self.timeout = timeout
            
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
        self.started = time.time()
        while time.time() < self.started + self.timeout:
            print "Test Task"
            time.sleep(1)
        self.completed = time.time()
        return True
