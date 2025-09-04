"""
Created on Sep 26, 2017

@author: riteshagarwal
"""

import concurrent.futures
import copy
import datetime
import importlib
import math
import os.path
import random
import re
import string
import threading
import time
import zlib
from collections import defaultdict
from random import sample, choice, randint
from subprocess import call

import requests

from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.task_manager import TaskManager
import global_vars
import mc_bin_client
import memcacheConstants
from BucketLib.BucketOperations import BucketHelper
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from Jython_tasks import sirius_task
from py_constants import CbServer
from py_constants.cb_constants import DocLoading
from Jython_tasks.task import \
    BucketCreateTask, \
    BucketCreateFromSpecTask, \
    ViewCreateTask, \
    ViewDeleteTask, \
    HibernationTask, \
    ViewQueryTask, DatabaseCreateTask, MonitorServerlessDatabaseScaling, \
    CloudHibernationTask
from SecurityLib.rbac import RbacUtil
from TestInput import TestInputSingleton, TestInputServer
from BucketLib.bucket import Bucket, Collection, Scope, Serverless
from capella_utils.dedicated import CapellaUtils as DedicatedCapellaUtils
from cb_constants.ClusterRun import ClusterRun
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.security.security_api import SecurityRestAPI
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaConstants, \
    MetaCrudParams
from common_lib import sleep, humanbytes, IDENTIFIER_TOKEN
from constants.cloud_constants.capella_cluster import CloudCluster
from couchbase_helper.data_analysis_helper import DataCollector, DataAnalyzer, \
    DataAnalysisResultAnalyzer
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import doc_generator, \
    sub_doc_generator, sub_doc_generator_for_edit
from error_simulation.cb_error import CouchbaseError
from global_vars import logger

from custom_exceptions.exception import StatsUnavailableException, \
    GetBucketInfoFailed
from membase.api.rest_client import RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper, \
    VBucketAwareMemcached
from shell_util.remote_connection import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from table_view import TableView
from testconstants import MAX_COMPACTION_THRESHOLD, \
    MIN_COMPACTION_THRESHOLD
from sdk_client3 import SDKClient
from constants.sdk_constants.java_client import SDKConstants
from couchbase_helper.tuq_generators import JsonGenerator
from StatsLib.StatsOperations import StatsHelper


"""
Create a set of bucket_parameters to be sent to all bucket_creation methods

Parameters:
    size - The size of the bucket to be created. (int)
    enable_replica_index - can be 0 or 1,
                           1 enables indexing of replica bucket data (int)
    replicas - The number of replicas for this bucket. (int)
    eviction_policy - The eviction policy for the bucket (String). Can be
        ephemeral bucket: noEviction or nruEviction
        non-ephemeral bucket: valueOnly or fullEviction.
    bucket_priority - Priority of the bucket:either none/low/high. (String)
    bucket_type - The type of bucket. (String)
    flushEnabled - Enable/Disable the flush functionality of the bucket. (int)
    lww = determine the conflict resolution type of the bucket. (Boolean)
"""

class JavaDocLoaderUtils(object):
    log = logger.get("test")
    bucket_util = None
    cluster_util = None
    process_concurrency = 20
    doc_loading_tm = None

    def __init__(self, bucket_util, cluster_util):
        JavaDocLoaderUtils.bucket_util = bucket_util
        JavaDocLoaderUtils.cluster_util = cluster_util
        JavaDocLoaderUtils.process_concurrency = TestInputSingleton.input.param("pc", 20)
        JavaDocLoaderUtils.doc_loading_tm = TaskManager(JavaDocLoaderUtils.process_concurrency)

    @staticmethod
    def generate_docs(doc_ops=None,
                      create_end=None, create_start=None,
                      update_end=None, update_start=None,
                      delete_end=None, delete_start=None,
                      expire_end=None, expire_start=None,
                      read_end=None, read_start=None,
                      bucket=None):
        bucket.create_end = 0
        bucket.create_start = 0
        bucket.read_end = 0
        bucket.read_start = 0
        bucket.update_end = 0
        bucket.update_start = 0
        bucket.delete_end = 0
        bucket.delete_start = 0
        bucket.expire_end = 0
        bucket.expire_start = 0
        try:
            bucket.final_items
        except:
            bucket.final_items = 0
        bucket.initial_items = bucket.final_items
        if not hasattr(bucket, "start"):
            bucket.start = 0
        if not hasattr(bucket, "end"):
            bucket.end = 0

        doc_ops = doc_ops or bucket.loadDefn.get("load_type")

        if "read" in doc_ops:
            bucket.read_start = read_start or 0
            bucket.read_end = read_end or bucket.loadDefn.get("num_items")//2

        if "update" in doc_ops:
            bucket.update_start = update_start or 0
            bucket.update_end = update_end or bucket.loadDefn.get("num_items")//2
            try:
                bucket.mutate
            except:
                bucket.mutate = 0
            bucket.mutate += 1

        if "delete" in doc_ops:
            bucket.delete_start = delete_start or bucket.start
            bucket.delete_end = delete_end or bucket.start + bucket.loadDefn.get("num_items")//2
            bucket.final_items -= (bucket.delete_end - bucket.delete_start) * bucket.loadDefn.get("collections") * bucket.loadDefn.get("scopes")

        if "expiry" in doc_ops:
            if bucket.loadDefn.get("maxttl") is None:
                bucket.loadDefn["maxttl"] = TestInputSingleton.input.param("maxttl", 10)
            bucket.expire_start = expire_start or bucket.end
            bucket.start = bucket.expire_start
            bucket.expire_end = expire_end or bucket.expire_start + bucket.loadDefn.get("num_items")//2
            bucket.end = bucket.expire_end

        if "create" in doc_ops:
            bucket.create_start = create_start if create_start is not None else bucket.end
            bucket.start = bucket.create_start

            bucket.create_end = create_end or bucket.end + (bucket.expire_end - bucket.expire_start) + (bucket.delete_end - bucket.delete_start)
            bucket.end = bucket.create_end

            bucket.final_items += (abs(bucket.create_end - bucket.create_start)) * bucket.loadDefn.get("collections") * bucket.loadDefn.get("scopes")
        print("================{}=================".format(bucket.name))
        print("Read Start: %s" % bucket.read_start)
        print("Read End: %s" % bucket.read_end)
        print("Update Start: %s" % bucket.update_start)
        print("Update End: %s" % bucket.update_end)
        print("Expiry Start: %s" % bucket.expire_start)
        print("Expiry End: %s" % bucket.expire_end)
        print("Delete Start: %s" % bucket.delete_start)
        print("Delete End: %s" % bucket.delete_end)
        print("Create Start: %s" % bucket.create_start)
        print("Create End: %s" % bucket.create_end)
        print("Final Start: %s" % bucket.start)
        print("Final End: %s" % bucket.end)
        print("================{}=================".format(bucket.name))

    @staticmethod
    def _loader_dict(cluster, buckets, overRidePattern=None,
                     key_prefix=None, key_size=None, key_type=None,
                     model=None, mockVector=None, base64=None,
                     process_concurrency=None, dim=None,
                     skip_default=True, mutate=0,
                     suppress_error_table=False,
                     track_failures=True):
        loader_map = dict()
        default_pattern = {"create": 100, "read": 0, "update": 0, "delete": 0, "expiry": 0}
        buckets = buckets or cluster.buckets
        for bucket in buckets:
            pattern = overRidePattern or bucket.loadDefn.get("doc_op_percentages", default_pattern)
            for scope in bucket.scopes.keys():
                for i, collection in enumerate(bucket.scopes[scope].collections.keys()):
                    workloads = bucket.loadDefn.get("collections_defn", [bucket.loadDefn])
                    valType = workloads[i % len(workloads)]["valType"]
                    dim = dim or workloads[i % len(workloads)].get("dim", 128)
                    key_prefix = key_prefix or bucket.loadDefn.get("key_prefix", "test_docs-")
                    key_size = key_size or bucket.loadDefn.get("key_size", 20)
                    key_type = key_type or bucket.loadDefn.get("key_type", "SimpleKey")
                    model = model or bucket.loadDefn.get("model", "Hotel")
                    mockVector = mockVector or bucket.loadDefn.get("mockVector", False)
                    base64 = base64 or bucket.loadDefn.get("base64", False)
                    process_concurrency = process_concurrency or bucket.loadDefn.get("process_concurrency", 10)
                    if scope == CbServer.system_scope:
                        continue
                    if collection == "_default" and scope == "_default" and skip_default:
                        continue
                    per_coll_ops = bucket.loadDefn.get("ops")//(len(bucket.scopes[scope].collections.keys()) - 1)
                    loader = SiriusCouchbaseLoader(
                        server_ip=cluster.master.ip, server_port=cluster.master.port,
                        username="Administrator", password="password",
                        bucket=bucket,
                        scope_name=scope, collection_name=collection,
                        key_prefix=key_prefix, key_size=key_size, doc_size=256,
                        key_type=key_type, value_type=valType,
                        create_percent=pattern["create"], read_percent=pattern["read"], update_percent=pattern["update"],
                        delete_percent=pattern["delete"], expiry_percent=pattern["expiry"],
                        create_start_index=bucket.create_start , create_end_index=bucket.create_end,
                        read_start_index=bucket.read_start, read_end_index=bucket.read_end,
                        update_start_index=bucket.update_start, update_end_index=bucket.update_end,
                        delete_start_index=bucket.delete_start, delete_end_index=bucket.delete_end,
                        expiry_start_index=bucket.expire_start, expiry_end_index=bucket.expire_end,
                        process_concurrency=process_concurrency, task_identifier="", ops=per_coll_ops,
                        suppress_error_table=suppress_error_table,
                        track_failures=track_failures,
                        mutate=mutate,
                        elastic=False, model=model, mockVector=mockVector, dim=dim, base64=base64)
                    loader_map.update({bucket.name+scope+collection: loader})
        return loader_map

    @staticmethod
    def wait_for_doc_load_completion(cluster, tasks, wait_for_stats=True, track_failures=True):
        for task in tasks:
            task.result = JavaDocLoaderUtils.doc_loading_tm.get_task_result(task)
            if not task.result:
                raise Exception("Task Failed: {}".format(task.thread_name))
        if wait_for_stats:
            JavaDocLoaderUtils.bucket_util._wait_for_stats_all_buckets(
                cluster, cluster.buckets, timeout=28800)
            if track_failures and cluster.type == "default":
                for bucket in cluster.buckets:
                    JavaDocLoaderUtils.bucket_util.verify_stats_all_buckets(
                        cluster, bucket.final_items, timeout=28800,
                        buckets=[bucket])

    @staticmethod
    def perform_load(cluster, buckets, wait_for_load=True,
                     validate_data=False, overRidePattern=None, skip_default=True,
                     wait_for_stats=True, mutate=0,
                     suppress_error_table=False,
                     track_failures=True):
        loader_map = JavaDocLoaderUtils._loader_dict(cluster, buckets,
                                                     overRidePattern, skip_default=skip_default,
                                                     mutate=mutate,
                                                     suppress_error_table=suppress_error_table,
                                                     track_failures=track_failures)
        tasks = list()
        for bucket in buckets:
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                for collection in bucket.scopes[scope].collections.keys():
                    if scope == CbServer.system_scope:
                        continue
                    if collection == "_default" and scope == "_default" and skip_default:
                        continue
                    loader = loader_map[bucket.name+scope+collection]
                    loader.create_doc_load_task()
                    JavaDocLoaderUtils.doc_loading_tm.add_new_task(loader)
                    tasks.append(loader)

        if wait_for_load:
            JavaDocLoaderUtils.wait_for_doc_load_completion(cluster, tasks, wait_for_stats)
        else:
            return tasks

        if validate_data:
            JavaDocLoaderUtils.data_validation(cluster, skip_default=skip_default)

    @staticmethod
    def load_sift_data(cluster=None, buckets=None, overRidePattern=None, skip_default=True,
                        wait_for_load=True,
                        validate_data=False,
                        wait_for_stats=True,
                        override_num_items=None,
                        mutate=0,
                        suppress_error_table=False,
                        track_failures=True):
        tasks = list()
        buckets = buckets or cluster.buckets
        coll_order = TestInputSingleton.input.param("coll_order", 1)
        default_pattern = {"create": 0, "update": 100, "delete": 0, "read": 0, "expiry": 0}
        for bucket in buckets:
            pattern = overRidePattern or bucket.loadDefn.get("doc_op_percentages", default_pattern)
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                collections = list(bucket.scopes[scope].collections.keys())
                if "_default" in collections:
                    collections.remove("_default")
                for i, collection in enumerate(sorted(collections)):
                    workloads = bucket.loadDefn.get("collections_defn", [bucket.loadDefn])
                    if coll_order == -1:
                        workloads = list(reversed(workloads))
                    workload = workloads[i % len(workloads)]
                    items = override_num_items or workload.get("num_items")
                    key_prefix = bucket.loadDefn.get("key_prefix")
                    key_size = bucket.loadDefn.get("key_size")
                    key_type = bucket.loadDefn.get("key_type")
                    model = bucket.loadDefn.get("model")
                    mockVector = bucket.loadDefn.get("mockVector")
                    base64 = bucket.loadDefn.get("base64")
                    process_concurrency = bucket.loadDefn.get("process_concurrency")
                    valType = workload["valType"]
                    dim = workload.get("dim", 128)
                    if collection == "_default" and scope == "_default" and skip_default:
                        continue
                    loader = SiriusCouchbaseLoader(
                        server_ip=cluster.master.ip, server_port=cluster.master.port,
                        generator=None, op_type=None,
                        username="Administrator", password="password",
                        bucket=bucket,
                        scope_name=scope, collection_name=collection,
                        key_prefix=key_prefix, key_size=key_size, doc_size=256,
                        key_type=key_type, value_type=valType,
                        create_percent=pattern["create"], read_percent=pattern["read"], update_percent=pattern["update"],
                        delete_percent=pattern["delete"], expiry_percent=pattern["expiry"],
                        create_start_index=0 , create_end_index=items,
                        read_start_index=0, read_end_index=items,
                        update_start_index=0, update_end_index=items,
                        delete_start_index=0, delete_end_index=items,
                        touch_start_index=0, touch_end_index=0,
                        replace_start_index=0, replace_end_index=0,
                        expiry_start_index=0, expiry_end_index=items,
                        process_concurrency=process_concurrency,
                        task_identifier="", ops=bucket.loadDefn.get("ops"),
                        suppress_error_table=suppress_error_table,
                        track_failures=track_failures,
                        mutate=mutate,
                        elastic=False, model=model, mockVector=mockVector, dim=dim, base64=base64,
                        base_vectors_file_path=bucket.loadDefn.get("baseFilePath", "/root/bigann_base.bvecs.gz"),
                        sift_url="ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz")
                    loader.create_doc_load_task()
                    JavaDocLoaderUtils.doc_loading_tm.add_new_task(loader)
                    tasks.append(loader)
                    i -= 1
        if wait_for_load:
            JavaDocLoaderUtils.wait_for_doc_load_completion(cluster, tasks, wait_for_stats)
        else:
            return tasks

        if validate_data:
            JavaDocLoaderUtils.data_validation(cluster, skip_default=skip_default)

    @staticmethod
    def load_data(cluster, buckets=None, overRidePattern=None,
                  wait_for_load=True,
                  wait_for_stats=True,
                  validate_data=False,
                  update=False,
                  mutate=0,
                  suppress_error_table=False,
                  track_failures=True,
                  override_num_items=None):
        buckets = buckets or cluster.buckets
        for bucket in buckets:
            JavaDocLoaderUtils.generate_docs(doc_ops=["create"],
                               create_start=0,
                               create_end=override_num_items or bucket.loadDefn.get("num_items"),
                               bucket=bucket)

        JavaDocLoaderUtils.perform_load(cluster=cluster,
                            buckets=buckets,
                            overRidePattern=overRidePattern,
                            validate_data=validate_data,
                            wait_for_load=wait_for_load,
                            wait_for_stats=wait_for_stats,
                            mutate=mutate,
                            suppress_error_table=suppress_error_table,
                            track_failures=track_failures)
        if update:
            for bucket in buckets:
                JavaDocLoaderUtils.generate_docs(doc_ops=["update"],
                                   update_start=0,
                                   update_end=override_num_items or bucket.loadDefn.get("num_items"),
                                   bucket=bucket)
            JavaDocLoaderUtils.perform_load(cluster=cluster,
                              buckets=buckets,
                              overRidePattern={"create": 0, "read": 0, "update": 100, "delete": 0, "expiry": 0},
                              validate_data=False,
                              wait_for_load=wait_for_load,
                              wait_for_stats=wait_for_stats,
                              mutate=mutate,
                              suppress_error_table=suppress_error_table,
                              track_failures=track_failures)

class DocLoaderUtils(object):
    log = logger.get("test")

    @staticmethod
    def check_if_exception_exists(received_exception, expected_exceptions):
        for expected_exception_str in expected_exceptions:
            if expected_exception_str in received_exception:
                return True
        return False

    @staticmethod
    def __get_required_num_from_percentage(collection_obj, target_percent, op_type):
        """
        To calculate the num_items for mutation for given collection_obj
        :param collection_obj: Object of Bucket.scope.collection
        :param target_percent: Percentage as given in spec
        :param: op_type: type of subdoc/doc crud operation
        :return: num_items (int) value derived using percentage value
        """
        if op_type in [DocLoading.Bucket.SubDocOps.REMOVE,
                       DocLoading.Bucket.SubDocOps.LOOKUP]:
            sub_doc_num_items = collection_obj.sub_doc_index[1] - collection_obj.sub_doc_index[0]
            return int((sub_doc_num_items * target_percent) / 100)
        else:
            return int((collection_obj.num_items * target_percent) / 100)

    @staticmethod
    def rewind_doc_index(collection_obj, op_type, doc_gen,
                         update_num_items=False):
        """
        Used to reset collection.num_items, in case of known failure scenarios
        :param collection_obj: Collection object from Bucket.scope.collection
        :param op_type: CRUD type used for by the generator
        :param doc_gen: Doc_generator to compute num_items from (start, end)
        :param update_num_items: If 'True', will update collection's num_items
                                 accordingly. This is optional since this is
                                 required only during rollback / validate_doc
                                 is not getting called.
        :return:
        """
        num_items = doc_gen.end - doc_gen.start
        if op_type == DocLoading.Bucket.DocOps.CREATE:
            collection_obj.doc_index = (collection_obj.doc_index[0],
                                        collection_obj.doc_index[1] - num_items)
            if update_num_items:
                collection_obj.num_items -= num_items
        elif op_type == DocLoading.Bucket.DocOps.DELETE:
            collection_obj.doc_index = (collection_obj.doc_index[0] - num_items,
                                        collection_obj.doc_index[1])
            if update_num_items:
                collection_obj.num_items += num_items
        elif op_type == DocLoading.Bucket.SubDocOps.INSERT:
            collection_obj.sub_doc_index = (
                collection_obj.sub_doc_index[0],
                collection_obj.sub_doc_index[1] - num_items)
        elif op_type == DocLoading.Bucket.SubDocOps.REMOVE:
            collection_obj.sub_doc_index = (
                collection_obj.sub_doc_index[0] - num_items,
                collection_obj.sub_doc_index[1])

    @staticmethod
    def get_doc_generator(op_type, collection_obj, num_items,
                          generic_key, mutation_num=0,
                          target_vbuckets="all", type="default",
                          doc_size=256, randomize_value=False,
                          randomize_doc_size=False, key_size=None):
        """
        Create doc generators based on op_type provided
        :param op_type: CRUD type
        :param collection_obj: Collection object from Bucket.scope.collection
        :param num_items: Targeted item count under the given collection
        :param generic_key: Doc_key to be used while doc generation
        :param mutation_num: Mutation number, used for doc validation task
        :param target_vbuckets: Target_vbuckets for which doc loading
                                should be done. Type: list / range()
        :param doc_size: Doc size to use for doc_generator
        :return: doc_generator object based on given inputs
        :param randomize_value: Randomize the data
        """
        if op_type == "create":
            start = collection_obj.doc_index[1]
            end = start + num_items
            if collection_obj.doc_index != (0, 0):
                collection_obj.num_items += (end - start)
            collection_obj.doc_index = (collection_obj.doc_index[0], end)
        elif op_type == "delete":
            start = collection_obj.doc_index[0]
            end = start + num_items
            collection_obj.doc_index = (end, collection_obj.doc_index[1])
            collection_obj.num_items -= (end - start)
        else:
            start = collection_obj.doc_index[0]
            end = start + num_items

        if op_type in [DocLoading.Bucket.DocOps.DELETE,
                       DocLoading.Bucket.DocOps.UPDATE,
                       DocLoading.Bucket.DocOps.REPLACE]:
            # if document is deleted/updated/replaced,
            # then it's corresponding subdoc
            # start index must also be changed
            if collection_obj.sub_doc_index[0] < end:
                collection_obj.sub_doc_index = \
                    (end, collection_obj.sub_doc_index[1])
                if collection_obj.sub_doc_index[1] < collection_obj.sub_doc_index[0]:
                    # no subdocs present
                    collection_obj.sub_doc_index = \
                        (collection_obj.sub_doc_index[0],
                         collection_obj.sub_doc_index[0])

        if target_vbuckets == "all":
            target_vbuckets = None
        else:
            # Target_vbuckets doc_gen require only total items to be created
            # Hence subtracting to get only the num_items back
            end -= start
        if type == "default":
            gen_docs = doc_generator(generic_key, start, end,
                                     doc_size=doc_size,
                                     target_vbucket=target_vbuckets,
                                     mutation_type=op_type,
                                     mutate=mutation_num,
                                     randomize_value=randomize_value,
                                     randomize_doc_size=randomize_doc_size,
                                     key_size=key_size)
        else:
            json_generator = JsonGenerator()
            if type == "employee":
                gen_docs = json_generator.generate_docs_employee_more_field_types(
                    generic_key, docs_per_day=end, start=start)
            else:
                gen_docs = json_generator.generate_earthquake_doc(
                    generic_key, docs_per_day=end, start=start)
        return gen_docs

    @staticmethod
    def get_subdoc_generator(op_type, collection_obj, num_items,
                             generic_key, target_vbuckets="all",
                             xattr_test=False):
        if target_vbuckets == "all":
            target_vbuckets = None

        if op_type == DocLoading.Bucket.SubDocOps.INSERT:
            start = collection_obj.sub_doc_index[1]
            end = start + num_items
            if end > collection_obj.doc_index[1]:
                # subdoc' end cannot be greater than last index of doc
                # otherwise it will result in docNotFound exceptions
                end = collection_obj.doc_index[1]
            collection_obj.sub_doc_index = (collection_obj.sub_doc_index[0],
                                            end)
            if target_vbuckets is not None:
                end -= start
            return sub_doc_generator(generic_key, start, end,
                                     target_vbucket=target_vbuckets,
                                     xattr_test=xattr_test)
        elif op_type == DocLoading.Bucket.SubDocOps.REMOVE:
            start = collection_obj.sub_doc_index[0]
            end = start + num_items
            collection_obj.sub_doc_index = (end,
                                            collection_obj.sub_doc_index[1])
            subdoc_gen_template_num = 2
        else:
            start = collection_obj.sub_doc_index[0]
            end = start + num_items
            if op_type == DocLoading.Bucket.SubDocOps.LOOKUP:
                subdoc_gen_template_num = 0
            else:
                subdoc_gen_template_num = choice([0, 1])
                if op_type == DocLoading.Bucket.SubDocOps.UPSERT:
                    # subdoc' end cannot be greater than last index of doc
                    # otherwise it will result in docNotFound exceptions
                    if end > collection_obj.doc_index[1]:
                        end = collection_obj.doc_index[1]
                        collection_obj.sub_doc_index = \
                            (collection_obj.sub_doc_index[0], end)
                    elif end > collection_obj.sub_doc_index[1]:
                        end = collection_obj.sub_doc_index[1]
                        collection_obj.sub_doc_index = \
                            (collection_obj.sub_doc_index[0], end)

        if target_vbuckets is not None:
            end -= start
        return sub_doc_generator_for_edit(generic_key, start, end,
                                          subdoc_gen_template_num,
                                          target_vbucket=target_vbuckets,
                                          xattr_test=xattr_test)

    @staticmethod
    def perform_doc_loading_for_spec(task_manager,
                                     cluster,
                                     buckets,
                                     crud_spec,
                                     batch_size=200,
                                     async_load=False,
                                     process_concurrency=1,
                                     print_ops_rate=True,
                                     load_using="default_loader",
                                     ops_rate=None):
        """
        Will load(only Creates) all given collections under all bucket objects.
        :param task_manager: TaskManager object used for task management
        :param cluster: Cluster object to fetch master node
                        (To create SDK connections)
        :param buckets: List of bucket objects considered for doc loading
        :param crud_spec: Dict for bucket-scope-collections with
                          appropriate doc_generator objects.
                          (Returned from create_doc_gens_for_spec function)
        :param batch_size: Batch size to use during doc_loading tasks
        :param async_load: (Boolean) To wait for doc_loading to finish or not
        :param process_concurrency: process_concurrency to use during doc_loading
        :param print_ops_rate: Bool value to enable/disable ops_rate printing
        :param load_using: Specify which loader to use (str)
        :return: List of doc_loading tasks
        """
        loader_spec = dict()
        for bucket_name, scope_dict in crud_spec.items():
            bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
            loader_spec[bucket] = scope_dict
        task = task_manager.async_load_gen_docs_from_spec(
            cluster, task_manager.jython_task_manager, loader_spec,
            batch_size=batch_size,
            process_concurrency=process_concurrency,
            print_ops_rate=print_ops_rate,
            load_using=load_using,
            start_task=True,
            ops_rate=ops_rate)
        if not async_load:
            task_manager.jython_task_manager.get_task_result(task)
        return task

    @staticmethod
    def create_doc_gens_for_spec(buckets, input_spec, mutation_num=0,
                                 op_details=dict()):
        """
        To perform the set of CRUD operations based on the given input spec.
        :param buckets: List of Bucket objects which needs to be considered
                        for doc loading
        :param input_spec: CRUD spec (JSON format)
        :param mutation_num: Mutation number, used for doc validation task
        :param op_details: Dict object returned from perform_tasks_from_spec()
        :return crud_spec: Dict for bucket-scope-collections with
                           appropriate doc_generator objects
        """

        def create_load_gens(spec,
                             sub_doc_gen=False,
                             is_xattr_test=False,
                             collection_recreated=False):
            for b_name, s_dict in spec.items():
                s_dict = s_dict["scopes"]
                if b_name not in crud_spec:
                    crud_spec[b_name] = dict()
                    crud_spec[b_name]["scopes"] = dict()
                bucket = BucketUtils.get_bucket_obj(buckets, b_name)
                for s_name, c_dict in s_dict.items():
                    if s_name not in crud_spec[b_name]["scopes"]:
                        crud_spec[b_name]["scopes"][s_name] = dict()
                        crud_spec[b_name]["scopes"][
                            s_name]["collections"] = dict()

                    c_dict = c_dict["collections"]
                    scope = bucket.scopes[s_name]
                    for c_name, _ in c_dict.items():
                        if c_name not in crud_spec[b_name]["scopes"][
                            s_name]["collections"]:
                            crud_spec[b_name]["scopes"][
                                s_name]["collections"][c_name] = dict()
                        c_crud_data = crud_spec[b_name]["scopes"][
                            s_name]["collections"][c_name]
                        collection = scope.collections[c_name]
                        target_ops = DocLoading.Bucket.DOC_OPS + ["cont_update", "cont_replace"]
                        if sub_doc_gen:
                            target_ops = DocLoading.Bucket.SUB_DOC_OPS
                        if collection_recreated:
                            target_ops = [DocLoading.Bucket.DocOps.CREATE]
                        for op_type in target_ops:
                            if collection_recreated:
                                num_items = \
                                    DocLoaderUtils \
                                        .__get_required_num_from_percentage(
                                        collection,
                                        100,
                                        op_type)
                            else:
                                num_items = \
                                    DocLoaderUtils \
                                        .__get_required_num_from_percentage(
                                        collection,
                                        spec_percent_data[op_type][0],
                                        op_type)

                            if num_items == 0:
                                continue

                            c_crud_data[op_type] = dict()
                            c_crud_data[op_type]["doc_ttl"] = doc_ttl
                            c_crud_data[op_type]["target_items"] = num_items
                            c_crud_data[op_type]["iterations"] = \
                                spec_percent_data[op_type][1]
                            c_crud_data[op_type]["sdk_timeout"] = sdk_timeout
                            c_crud_data[op_type]["sdk_timeout_unit"] = \
                                sdk_timeout_unit
                            c_crud_data[op_type]["durability_level"] = \
                                durability_level
                            c_crud_data[op_type]["skip_read_on_error"] = \
                                skip_read_on_error
                            c_crud_data[op_type]["suppress_error_table"] = \
                                suppress_error_table
                            c_crud_data[op_type]["skip_read_success_results"] = \
                                skip_read_success_results
                            c_crud_data[op_type]["ignore_exceptions"] = \
                                ignore_exceptions
                            c_crud_data[op_type]["retry_exceptions"] = \
                                retry_exceptions
                            if type(doc_gen_type) is list:
                                random.seed(c_name)
                                c_crud_data[op_type]["doc_gen_type"] = \
                                    random.choice(doc_gen_type)
                            else:
                                c_crud_data[op_type]["doc_gen_type"] = doc_gen_type
                            if op_type in DocLoading.Bucket.DOC_OPS:
                                c_crud_data[op_type]["doc_gen"] = \
                                    DocLoaderUtils.get_doc_generator(
                                        op_type,
                                        collection,
                                        num_items,
                                        doc_key,
                                        doc_size=doc_size,
                                        target_vbuckets=target_vbs,
                                        mutation_num=mutation_num,
                                        type=c_crud_data[op_type]["doc_gen_type"],
                                        randomize_value=randomize_value,
                                        randomize_doc_size=randomize_doc_size,
                                        key_size=doc_key_size)
                            else:
                                c_crud_data[op_type]["xattr_test"] = \
                                    is_xattr_test
                                c_crud_data[op_type]["doc_gen"] = \
                                    DocLoaderUtils.get_subdoc_generator(
                                        op_type,
                                        collection,
                                        num_items,
                                        doc_key,
                                        target_vbuckets=target_vbs,
                                        xattr_test=is_xattr_test)

        crud_spec = dict()
        spec_percent_data = dict()

        num_collection_to_load = input_spec.get(
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD, 1)
        num_scopes_to_consider = input_spec.get(
            MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD, 1)
        num_buckets_to_consider = input_spec.get(
            MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD, 1)

        # Create dict if not provided by user
        if "doc_crud" not in input_spec.keys():
            input_spec["doc_crud"] = dict()
        if "subdoc_crud" not in input_spec:
            input_spec["subdoc_crud"] = dict()

        # Fetch common doc_key to use while doc_loading
        doc_key = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.COMMON_DOC_KEY, "test_docs")
        # Fetch doc_size to use for doc_loading
        doc_size = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.DOC_SIZE, 256)
        doc_key_size = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.DOC_KEY_SIZE, len(doc_key)+8)
        # Fetch randomize_value to use for doc_loading
        randomize_value = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.RANDOMIZE_VALUE, False)
        randomize_doc_size = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.RANDOMIZE_DOC_SIZE, False)

        ignore_exceptions = input_spec.get(
            MetaCrudParams.IGNORE_EXCEPTIONS, [])
        retry_exceptions = input_spec.get(
            MetaCrudParams.RETRY_EXCEPTIONS, [])
        doc_gen_type = input_spec.get(
            MetaCrudParams.DOC_GEN_TYPE, "default")

        # Fetch doc_loading options to use while doc_loading
        doc_ttl = input_spec.get(MetaCrudParams.DOC_TTL, 0)
        sdk_timeout = input_spec.get(
            MetaCrudParams.SDK_TIMEOUT, 60)
        sdk_timeout_unit = input_spec.get(
            MetaCrudParams.SDK_TIMEOUT_UNIT, "seconds")
        durability_level = input_spec.get(
            MetaCrudParams.DURABILITY_LEVEL, "")
        skip_read_on_error = input_spec.get(
            MetaCrudParams.SKIP_READ_ON_ERROR, False)
        suppress_error_table = input_spec.get(
            MetaCrudParams.SUPPRESS_ERROR_TABLE, False)
        skip_read_success_results = input_spec.get(
            MetaCrudParams.SKIP_READ_SUCCESS_RESULTS, False)
        target_vbs = input_spec.get(
            MetaCrudParams.TARGET_VBUCKETS, "all")

        # Fetch  number of items to be loaded for newly created collections
        num_items_for_new_collections = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS, 0)
        # Fetch CRUD percentage from given spec
        spec_percent_data["create"] = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION, 0)
        spec_percent_data["update"] = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION, 0)
        spec_percent_data["delete"] = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION, 0)
        spec_percent_data["replace"] = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION, 0)
        spec_percent_data["read"] = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION, 0)
        spec_percent_data["touch"] = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.TOUCH_PERCENTAGE_PER_COLLECTION, 0)

        spec_percent_data["cont_update"] = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.CONT_UPDATE_PERCENT_PER_COLLECTION, (0, 0))
        spec_percent_data["cont_replace"] = input_spec["doc_crud"].get(
            MetaCrudParams.DocCrud.CONT_REPLACE_PERCENT_PER_COLLECTION, (0, 0))

        # Fetch sub_doc CRUD percentage from given spec
        xattr_test = input_spec["subdoc_crud"].get(
            MetaCrudParams.SubDocCrud.XATTR_TEST, False)
        spec_percent_data["insert"] = input_spec["subdoc_crud"].get(
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION, 0)
        spec_percent_data["upsert"] = input_spec["subdoc_crud"].get(
            MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION, 0)
        spec_percent_data["remove"] = input_spec["subdoc_crud"].get(
            MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION, 0)
        spec_percent_data["lookup"] = input_spec["subdoc_crud"].get(
            MetaCrudParams.SubDocCrud.LOOKUP_PER_COLLECTION, 0)

        for op_type in list(spec_percent_data.keys()):
            if isinstance(spec_percent_data[op_type], int):
                spec_percent_data[op_type] = (spec_percent_data[op_type], 1)
        exclude_dict = dict()
        if 'skip_dict' in input_spec:
            exclude_dict = input_spec['skip_dict']
        doc_crud_spec = BucketUtils.get_random_collections(
            buckets,
            num_collection_to_load,
            num_scopes_to_consider,
            num_buckets_to_consider,
            exclude_from=exclude_dict)
        sub_doc_crud_spec = BucketUtils.get_random_collections(
            buckets,
            num_collection_to_load,
            num_scopes_to_consider,
            num_buckets_to_consider,
            exclude_from=exclude_dict)

        create_load_gens(doc_crud_spec)

        # Add new num_items for newly added collections
        for bucket_name, scope_dict in op_details["collections_added"].items():
            b_obj = BucketUtils.get_bucket_obj(buckets, bucket_name)
            for scope_name, col_dict in scope_dict["scopes"].items():
                for col_name, _ in col_dict["collections"].items():
                    b_obj.scopes[
                        scope_name].collections[
                        col_name].num_items = num_items_for_new_collections

        create_load_gens(op_details["collections_added"],
                         collection_recreated=True)
        create_load_gens(sub_doc_crud_spec,
                         sub_doc_gen=True,
                         is_xattr_test=xattr_test)
        return crud_spec

    @staticmethod
    def validate_crud_task_per_collection(cluster, bucket, scope_name,
                                          collection_name, c_data,
                                          load_using="default_loader"):
        # Table objects to print data
        table_headers = ["Initial exception", "Key"]
        failure_table_header = table_headers + ["Retry exception"]
        ignored_keys_table = TableView(DocLoaderUtils.log.debug)
        retry_succeeded_table = TableView(DocLoaderUtils.log.debug)
        retry_failed_table = TableView(DocLoaderUtils.log.error)
        unwanted_retry_succeeded_table = TableView(DocLoaderUtils.log.error)
        unwanted_retry_failed_table = TableView(DocLoaderUtils.log.error)

        ignored_keys_table.set_headers(table_headers)
        retry_succeeded_table.set_headers(table_headers)
        retry_failed_table.set_headers(failure_table_header)
        unwanted_retry_succeeded_table.set_headers(table_headers)
        unwanted_retry_failed_table.set_headers(failure_table_header)

        exception_patterns = [".*(com\.[a-zA-Z0-9\.]+)", "([a-zA-Z0-9]+)"]

        if load_using != "default_loader":
            return

        # Fetch client for retry operations
        client = cluster.sdk_client_pool.get_client_for_bucket(
            bucket, scope_name, collection_name)
        doc_data = dict()
        subdoc_data = dict()
        for op_type, op_data in c_data.items():
            if op_type in DocLoading.Bucket.DOC_OPS:
                doc_data[op_type] = op_data
            else:
                subdoc_data[op_type] = op_data
        for data in [doc_data, subdoc_data]:
            for op_type, op_data in data.items():
                failed_keys = list(op_data["fail"].keys())

                # New dicts to filter failures based on retry strategy
                op_data["unwanted"] = dict()
                op_data["retried"] = dict()
                op_data["unwanted"]["fail"] = dict()
                op_data["unwanted"]["success"] = dict()
                op_data["retried"]["fail"] = dict()
                op_data["retried"]["success"] = dict()

                if failed_keys:
                    keys_to_remove = []
                    for key, failed_doc in op_data["fail"].items():
                        is_key_to_ignore = False
                        exception = failed_doc["error"]
                        for exception_pattern in exception_patterns:
                            pattern_match = re.match(exception_pattern,
                                                     str(exception))
                            if pattern_match:
                                initial_exception = pattern_match.group(1)
                                break

                        for tem_exception in op_data["ignore_exceptions"]:
                            if not isinstance(tem_exception, list):
                                tem_exception = [tem_exception]
                            for t_exception in tem_exception:
                                if str(exception).find(t_exception) != -1:
                                    if op_type == DocLoading.Bucket.DocOps.CREATE:
                                        bucket.scopes[scope_name] \
                                            .collections[collection_name].num_items \
                                            -= 1
                                    elif op_type == DocLoading.Bucket.DocOps.DELETE:
                                        bucket.scopes[scope_name] \
                                            .collections[collection_name].num_items \
                                            += 1
                                    is_key_to_ignore = True
                                    break_loop = True
                                    break
                            if break_loop:
                                break

                        if is_key_to_ignore:
                            keys_to_remove.append(key)
                            ignored_keys_table.add_row([initial_exception, key])
                            continue

                        ambiguous_state = False
                        exceptions_to_check = [
                            SDKException.DurabilityAmbiguousException,
                            SDKException.AuthenticationException,
                            SDKException.TimeoutException,
                        ]
                        for tem_exception in exceptions_to_check:
                            if SDKException.check_if_exception_exists(
                                    tem_exception, str(exception)):
                                ambiguous_state = True
                                break
                        else:
                            if "CHANNEL_CLOSED_WHILE_IN_FLIGHT" \
                                    in str(exception):
                                ambiguous_state = True

                        if op_type in DocLoading.Bucket.DOC_OPS:
                            result = client.crud(
                                op_type, key, failed_doc["value"],
                                exp=op_data["doc_ttl"],
                                durability=op_data["durability_level"],
                                timeout=op_data["sdk_timeout"],
                                time_unit=op_data["sdk_timeout_unit"])
                        else:
                            result = dict()
                            result['status'] = True
                            for tup in failed_doc["path_val"]:
                                if op_type in [DocLoading.Bucket.SubDocOps.LOOKUP,
                                               DocLoading.Bucket.SubDocOps.REMOVE]:
                                    tup = tup[0]
                                ind_result = client.crud(
                                    op_type, key, tup,
                                    exp=op_data["doc_ttl"],
                                    durability=op_data["durability_level"],
                                    timeout=op_data["sdk_timeout"],
                                    time_unit=op_data["sdk_timeout_unit"],
                                    xattr=op_data["xattr_test"])
                                fail = ind_result[1]
                                if not fail:
                                    continue
                                else:
                                    result['status'] = False
                                    result['error'] = fail[key]["error"]
                                    break

                        retry_strategy = "unwanted"
                        for ex in op_data["retry_exceptions"]:
                            if str(exception).find(ex) != -1:
                                retry_strategy = "retried"
                                break

                        if result["status"] \
                                or (ambiguous_state
                                    and SDKException.DocumentExistsException
                                    in result["error"]
                                    and op_type in ["create", "update"]) \
                                or (ambiguous_state
                                    and SDKException.PathExistsException
                                    in result["error"]
                                    and op_type in [DocLoading.Bucket.SubDocOps.INSERT]) \
                                or (ambiguous_state
                                    and SDKException.DocumentNotFoundException
                                    in result["error"]
                                    and op_type == "delete") \
                                or (ambiguous_state
                                    and SDKException.PathNotFoundException
                                    in result["error"]
                                    and op_type == DocLoading.Bucket.SubDocOps.REMOVE):
                            op_data[retry_strategy]["success"][key] = \
                                result
                            if retry_strategy == "retried":
                                keys_to_remove.append(key)
                                target_tbl = retry_succeeded_table
                            else:
                                target_tbl = unwanted_retry_succeeded_table
                            tbl_row = [initial_exception, key]
                        else:
                            if op_type == DocLoading.Bucket.DocOps.CREATE:
                                bucket.scopes[scope_name] \
                                    .collections[collection_name].num_items \
                                    -= 1
                            elif op_type == DocLoading.Bucket.DocOps.DELETE:
                                bucket.scopes[scope_name] \
                                    .collections[collection_name].num_items \
                                    += 1
                            op_data[retry_strategy]["fail"][key] = result
                            if retry_strategy == "retried":
                                target_tbl = retry_failed_table
                            else:
                                target_tbl = unwanted_retry_failed_table
                            retry_exception = re.match(
                                exception_pattern, str(result["error"])).group(1)
                            tbl_row = [initial_exception, key, retry_exception]
                        target_tbl.add_row(tbl_row)
                    # Remove keys that should be ignored or succeeded in retry after iteration
                    for key in keys_to_remove:
                        op_data["fail"].pop(key)
                    gen_str = "%s:%s:%s - %s" \
                              % (bucket.name, scope_name, collection_name,
                                 op_type)
                    #                 ignored_keys_table.display(
                    #                     "%s keys ignored from retry" % gen_str)
                    #                 retry_succeeded_table.display(
                    #                     "%s keys succeeded after expected retry" % gen_str)
                    retry_failed_table.display(
                        "%s keys failed after expected retry" % gen_str)
                    unwanted_retry_succeeded_table.display(
                        "%s unwanted exception keys succeeded in retry" % gen_str)
                    unwanted_retry_failed_table.display(
                        "%s unwanted exception keys failed in retry" % gen_str)

        # Release the acquired client
        cluster.sdk_client_pool.release_client(client)

    @staticmethod
    def validate_doc_loading_results(cluster, doc_loading_task):
        """
        :param cluster: Cluster object holding sdk_client_pool variable
        :param doc_loading_task: Task object returned from
                                 DocLoaderUtils.perform_doc_loading_for_spec
        :return:
        """
        c_validation_threads = list()
        crud_validation_function = \
            DocLoaderUtils.validate_crud_task_per_collection
        for bucket_obj, scope_dict in doc_loading_task.loader_spec.items():
            for s_name, collection_dict in scope_dict["scopes"].items():
                for c_name, c_dict in collection_dict["collections"].items():
                    c_thread = threading.Thread(
                        target=crud_validation_function,
                        args=[cluster, bucket_obj, s_name, c_name, c_dict,
                              doc_loading_task.load_using])
                    c_thread.start()
                    c_validation_threads.append(c_thread)

        # Wait for all threads to complete
        for c_thread in c_validation_threads:
            c_thread.join()

        # Set doc_loading result based on the retry outcome
        doc_loading_task.result = True
        for _, scope_dict in doc_loading_task.loader_spec.items():
            for _, collection_dict in scope_dict["scopes"].items():
                for _, c_dict in collection_dict["collections"].items():
                    for op_type, op_data in c_dict.items():
                        if op_data["fail"]:
                            doc_loading_task.result = False
                            break

    @staticmethod
    def update_num_items_based_on_expired_docs(buckets, doc_loading_task,
                                               ttl_level="document"):
        """
        :param buckets: List of buckets to consider to fetch the TTL values
        :param doc_loading_task: Task object returned from
                                 DocLoaderUtils.create_doc_gens_for_spec call
        :param ttl_level: Target TTL value to consider.
                          Valid options: document, collection, bucket
                          Default is "document". TTL set during doc_loading.
        :return:
        """
        # Variable used to sleep until doc_expiry happens
        doc_expiry_time = 0
        BucketUtils.log.debug(
            "Updating collection's num_items as per TTL settings")
        if ttl_level == "document":
            for bucket, scope_dict in doc_loading_task.loader_spec.items():
                for s_name, c_dict in scope_dict["scopes"].items():
                    num_items_updated = False
                    for c_name, c_data in c_dict["collections"].items():
                        c_obj = bucket.scopes[s_name].collections[c_name]
                        for op_type, op_data in c_data.items():
                            if op_data["doc_ttl"] > doc_expiry_time:
                                doc_expiry_time = op_data["doc_ttl"]

                            if op_data["doc_ttl"] != 0 \
                                    and op_type in [DocLoading.Bucket.DocOps.CREATE,
                                                    DocLoading.Bucket.DocOps.TOUCH,
                                                    DocLoading.Bucket.DocOps.UPDATE,
                                                    DocLoading.Bucket.DocOps.REPLACE]:
                                # Calculate num_items affected for this CRUD
                                affected_items = op_data["doc_gen"].end \
                                                 - op_data["doc_gen"].start
                                # Resetting the doc_index to make sure
                                # further Updates/Deletes won't break the
                                # Doc loading tasks
                                if op_type == DocLoading.Bucket.DocOps.CREATE:
                                    reset_end = c_obj.doc_index[1] \
                                                - affected_items
                                    c_obj.doc_index = (c_obj.doc_index[0],
                                                       reset_end)
                                    c_obj.num_items -= affected_items
                                # We use same doc indexes for
                                # UPDATE/TOUCH/REPLACE ops. So updating
                                # the data only once is a correct approach
                                elif not num_items_updated:
                                    num_items_updated = True
                                    new_start = c_obj.doc_index[0] \
                                                + affected_items
                                    c_obj.doc_index = (new_start,
                                                       c_obj.doc_index[1])
                                    c_obj.num_items -= affected_items
        elif ttl_level == "collection":
            for bucket in buckets:
                for _, scope in bucket.scopes.items():
                    for _, collection in scope.collections.items():
                        if collection.maxTTL > doc_expiry_time:
                            doc_expiry_time = collection.maxTTL
                            collection.num_items = 0
        elif ttl_level == "bucket":
            for bucket in buckets:
                if bucket.maxTTL > doc_expiry_time:
                    doc_expiry_time = bucket.maxTTL
                for _, scope in bucket.scopes.items():
                    for _, collection in scope.collections.items():
                        collection.num_items = 0
        else:
            DocLoaderUtils.log.error("Unsupported ttl_level value: %s"
                                     % ttl_level)

        sleep(doc_expiry_time, "Wait for docs to get expire as per TTL")

    @staticmethod
    def run_scenario_from_spec(task_manager, cluster, buckets,
                               input_spec,
                               batch_size=200,
                               mutation_num=1,
                               async_load=False,
                               validate_task=True,
                               process_concurrency=1,
                               print_ops_rate=True,
                               load_using="default_loader",
                               ops_rate=None):
        """
        :param task_manager: TaskManager object used for task management
        :param cluster: Cluster object to fetch master node
                        To create SDK connections / REST client
        :param buckets: List of bucket objects considered for doc loading
        :param input_spec: CRUD spec (JSON format)
        :param batch_size: Batch size to use during doc_loading tasks
        :param mutation_num: Mutation field value to be used in doc_generator
        :param async_load: (Bool) To wait for doc_loading to finish or not
        :param validate_task: (Bool) To validate doc_loading results or not
        :param process_concurrency: process_concurrency to use during doc_loading
        :param print_ops_rate: Bool value to enable/disable ops_rate printing
        :param load_using: Specify which loader to use (str)
        :return doc_loading_task: Task object returned from
                                  DocLoaderUtils.perform_doc_loading_for_spec
        """
        op_details = BucketUtils.perform_tasks_from_spec(cluster,
                                                         buckets,
                                                         input_spec)
        crud_spec = DocLoaderUtils.create_doc_gens_for_spec(buckets,
                                                            input_spec,
                                                            mutation_num,
                                                            op_details)
        doc_loading_task = DocLoaderUtils.perform_doc_loading_for_spec(
            task_manager,
            cluster,
            buckets,
            crud_spec,
            batch_size=batch_size,
            async_load=async_load,
            process_concurrency=process_concurrency,
            print_ops_rate=print_ops_rate,
            load_using=load_using,
            ops_rate=ops_rate)
        if not async_load and validate_task:
            DocLoaderUtils.validate_doc_loading_results(cluster,
                                                        doc_loading_task)
        return doc_loading_task

    @staticmethod
    def get_workload_settings(
            key="test_doc", key_size=10, doc_size=1,
            create_perc=0, read_perc=0, update_perc=0, delete_perc=0,
            expiry_perc=0, process_concurrency=1, ops_rate=1,
            load_type=None, key_type="SimpleKey", value_type="SimpleValue",
            validate=False, gtm=False, deleted=False, mutated=0,
            create_start=0, create_end=0, update_start=0, update_end=0,
            read_start=0, read_end=0, delete_start=0, delete_end=0,
            expiry_start=0, expiry_end=0):
        """
        :param key:
        :param key_size:
        :param doc_size:
        :param create_perc:
        :param read_perc:
        :param update_perc:
        :param delete_perc:
        :param expiry_perc:
        :param process_concurrency:
        :param ops_rate:
        :param load_type:
        :param key_type:
        :param value_type:
        :param validate:
        :param gtm:
        :param deleted:
        :param mutated:
        :param create_start:
        :param create_end:
        :param update_start:
        :param update_end:
        :param read_start:
        :param read_end:
        :param delete_start:
        :param delete_end:
        :param expiry_start:
        :param expiry_end:
        :return: WorkLoadSettings object

        Note: Java dependent
        """
        hm = HashMap()
        hm.putAll({DRConstants.create_s: create_start,
                   DRConstants.create_e: create_end,
                   DRConstants.update_s: update_start,
                   DRConstants.update_e: update_end,
                   DRConstants.expiry_s: expiry_start,
                   DRConstants.expiry_e: expiry_end,
                   DRConstants.delete_s: delete_start,
                   DRConstants.delete_e: delete_end,
                   DRConstants.read_s: read_start,
                   DRConstants.read_e: read_end})
        dr = DocRange(hm)
        ws = WorkLoadSettings(key, key_size, doc_size,
                              create_perc, read_perc, update_perc,
                              delete_perc, expiry_perc,
                              process_concurrency, ops_rate,
                              load_type, key_type, value_type, validate,
                              gtm, deleted, mutated)
        ws.dr = dr
        return ws

    @staticmethod
    def initialize_java_sdk_client_pool():
        return None

    @staticmethod
    def perform_doc_loading(task_manager, loader_map, cluster, buckets=None,
                            durability_level=SDKConstants.DurabilityLevel.NONE,
                            maxttl=0, ttl_time_unit="seconds",
                            process_concurrency=1,
                            track_failures=True,
                            async_load=True, validate_results=False,
                            retries=0):
        def get_bucket_obj(b_name):
            for t_bucket in buckets:
                if t_bucket.name == b_name:
                    return t_bucket
        log = DocLoaderUtils.log
        master = None
        result = True
        if buckets is None:
            buckets = cluster.buckets
        if cluster.sdk_client_pool is None and cluster.master:
            master = Server(cluster.master.ip, cluster.master.port,
                            cluster.master.rest_username,
                            cluster.master.rest_password,
                            str(cluster.master.memcached_port))
        tasks = list()
        while process_concurrency > 0:
            for loader_map_key, dg in loader_map.items():
                bucket_name, scope, collection = loader_map_key.split(":")
                bucket = get_bucket_obj(bucket_name)
                task_name = "Loader_%s_%s_%s_%s_%s" \
                            % (bucket_name, scope, collection,
                               process_concurrency, "%s" % time())
                if cluster.sdk_client_pool:
                    task = WorkLoadGenerate(
                        task_name, dg, cluster.sdk_client_pool,
                        durability_level, maxttl,
                        ttl_time_unit, track_failures, retries)
                    task.set_collection_for_load(
                        bucket.name, scope, collection)
                else:
                    if bucket.serverless \
                            and bucket.serverless.nebula_endpoint:
                        nebula = bucket.serverless.nebula_endpoint
                        log.info("Using Nebula endpoint %s" % nebula.srv)
                        master = Server(nebula.srv, nebula.port,
                                        nebula.rest_username,
                                        nebula.rest_password,
                                        str(nebula.memcached_port))
                    client = NewSDKClient(master, bucket.name,
                                          scope, collection)
                    client.initialiseSDK()
                    sleep(1, "Wait for SDK to warmup")
                    task = WorkLoadGenerate(
                        task_name, dg, client, durability_level,
                        maxttl, ttl_time_unit, track_failures, retries)
                tasks.append(task)
                task_manager.submit(task)
                process_concurrency -= 1

        if async_load is False:
            DocLoaderUtils.wait_for_doc_load_completion(task_manager, tasks)

            if validate_results:
                result = DocLoaderUtils.data_validation(
                    task_manager, loader_map, cluster, buckets,
                    process_concurrency=process_concurrency, ops_rate=100)

        return result, tasks

    @staticmethod
    def wait_for_doc_load_completion(task_manager, tasks):
        result = True
        log = DocLoaderUtils.log
        for task in tasks:
            task_manager.getTaskResult(task)
            task.result = True
            unique_str = "%s:%s:%s:" % (task.sdk.bucket, task.sdk.scope,
                                        task.sdk.collection)
            for optype, failures in task.failedMutations.items():
                for failure in failures:
                    if failure is not None:
                        log.error("Test Retrying {}: {}{} -> {}".format(optype, unique_str, failure.id(), failure.err().getClass().getSimpleName()))
                        try:
                            if optype == DocLoading.Bucket.DocOps.CREATE:
                                task.docops.insert(
                                    failure.id(), failure.document(),
                                    task.sdk.connection, task.setOptions)
                            elif optype == DocLoading.Bucket.DocOps.UPDATE:
                                task.docops.upsert(
                                    failure.id(), failure.document(),
                                    task.sdk.connection, task.upsertOptions)
                            elif optype == DocLoading.Bucket.DocOps.DELETE:
                                task.docops.delete(
                                    failure.id(),
                                    task.sdk.connection, task.removeOptions)
                        except (ServerOutOfMemoryException, TimeoutException) as e:
                            log.error("Retry %s failed for key: %s - %s"
                                      % (optype, failure.id(), e))
                            task.result = False
                        except (DocumentNotFoundException, DocumentExistsException) as e:
                            log.error(e)
                            pass
            if task.sdkClientPool is None:
                try:
                    task.sdk.disconnectCluster()
                except Exception as e:
                    print(e)
            if task.result is False:
                log.error("Task failed: %s" % task.taskName)
                result = False
        return result

    @staticmethod
    def data_validation(task_manager, loader_map, cluster, buckets=None,
                        process_concurrency=1, ops_rate=100,
                        max_ttl=0, ttl_timeunit="seconds",
                        track_failures=True):
        def get_bucket_obj(b_name):
            for t_bucket in buckets:
                if t_bucket.name == b_name:
                    return t_bucket

        def update_validation_map(b_name, s_name, c_name, doc_op,
                                  start_index, end_index, docs_deleted=False):
            hm = HashMap()
            hm.putAll({DRConstants.read_s: start_index,
                       DRConstants.read_e: end_index})
            ws = DocLoaderUtils.get_workload_settings(
                key=workload_setting.keyPrefix,
                key_size=workload_setting.keySize,
                doc_size=workload_setting.docSize,
                read_perc=100,
                deleted=docs_deleted,
                ops_rate=ops_rate,
                process_concurrency=process_concurrency)
            ws.dr = DocRange(hm)
            dg = DocumentGenerator(ws,
                                   workload_setting.keyType,
                                   workload_setting.valueType)
            v_key = "%s:%s:%s:%s" % (b_name, s_name, c_name, doc_op)
            validation_map.update({v_key: dg})

        master = None
        result = True
        validation_map = dict()
        buckets = cluster.buckets if buckets is None else buckets
        log = DocLoaderUtils.log
        log.info("Creating doc_gens for running validations")
        for loader_map_key, doc_gen in loader_map.items():
            bucket_name, scope, collection = loader_map_key.split(":")
            workload_setting = doc_gen.get_work_load_settings()
            doc_range = workload_setting.dr
            if workload_setting.creates > 0:
                start, end = doc_range.create_s, doc_range.create_e
                update_validation_map(bucket_name, scope, collection,
                                      DocLoading.Bucket.DocOps.CREATE,
                                      start, end)
            if workload_setting.updates > 0:
                start, end = doc_range.update_s, doc_range.update_e
                update_validation_map(bucket_name, scope, collection,
                                      DocLoading.Bucket.DocOps.UPDATE,
                                      start, end)
            if workload_setting.deletes > 0:
                start, end = doc_range.delete_s, doc_range.delete_e
                update_validation_map(bucket_name, scope, collection,
                                      DocLoading.Bucket.DocOps.DELETE,
                                      start, end, docs_deleted=True)

        log.info("Validating Active/Replica Docs")
        if cluster.sdk_client_pool is None and cluster.master:
            master = Server(cluster.master.ip, cluster.master.port,
                            cluster.master.rest_username,
                            cluster.master.rest_password,
                            str(cluster.master.memcached_port))

        tasks = list()
        while process_concurrency > 0:
            for map_key, doc_gen in validation_map.items():
                bucket_name, scope, collection, op_type = map_key.split(":")
                bucket = get_bucket_obj(bucket_name)

                task_name = "Validate_%s_%s_%s_%s_%s_%s" \
                            % (bucket_name, scope, collection,
                               op_type, str(process_concurrency),
                               time.time())
                if cluster.sdk_client_pool:
                    task = WorkLoadGenerate(
                        task_name, doc_gen, cluster.sdk_client_pool, "NONE",
                        max_ttl, ttl_timeunit, track_failures, 0)
                    task.set_collection_for_load(bucket_name, scope,
                                                 collection)
                else:
                    if bucket.serverless \
                            and bucket.serverless.nebula_endpoint:
                        nebula = bucket.serverless.nebula_endpoint
                        DocLoaderUtils.log.info(
                            "Nebula endpoint: %s" % nebula.srv)
                        master = Server(nebula.srv, nebula.port,
                                        nebula.rest_username,
                                        nebula.rest_password,
                                        str(nebula.memcached_port))

                    client = NewSDKClient(master, bucket_name, scope,
                                          collection)
                    client.initialiseSDK()
                    sleep(1, "Wait for SDK to warmup")
                    task = WorkLoadGenerate(
                        task_name, doc_gen, client, "NONE",
                        max_ttl, ttl_timeunit, track_failures, 0)
                task_manager.submit(task)
                tasks.append(task)
                process_concurrency -= 1
        for task in tasks:
            task_manager.getTaskResult(task)
            if cluster.sdk_client_pool is None:
                try:
                    task.sdk.disconnectCluster()
                except Exception as e:
                    DocLoaderUtils.log.critical(e)
            if task.result is False:
                DocLoaderUtils.log.error("Validation failed: %s"
                                         % task.taskName)
                result = False
        return result


class CollectionUtils(DocLoaderUtils):
    @staticmethod
    def get_range_scan_results(range_scan_result, expect_failure, log):
        """ segregates range scan count fails with exception and visualise
        them in tabular format accepts list of dict in following format
        [failure_map = {
            "failure_in_scan": Bool,
            "prefix_scan": dict,
            "sampling_scan":dict,
            "range_scan": dict,
            "exception": list}]
        """
        log.debug("range scan result %s" % str(range_scan_result))
        if len(range_scan_result) == 0:
            return True
        table = TableView(log.info)
        table.set_headers(["Type", "Scan Detail"])
        for result in range_scan_result:
            if len(result["prefix_scan"]) > 0:
                table.add_row(["prefix_scan", result["prefix_scan"]])
            if len(result["range_scan"]) > 0:
                table.add_row(["range_scan", result["range_scan"]])
            if len(result["sampling_scan"]) > 0:
                table.add_row(["sampling_scan", result["sampling_scan"]])

        table.display("Range scan results")
        if expect_failure:
            return True
        return False

    @staticmethod
    def get_collection_obj(scope, collection_name):
        """
        Fetch the target collection object under the given scope object

        :param scope: Scope object to iterate through all collections
        :param collection_name: Target collection name to fetch the object
        """
        collection = None
        if collection_name in scope.collections.keys():
            collection = scope.collections[collection_name]
        return collection

    @staticmethod
    def get_active_collections(bucket, scope_name, only_names=False):
        """
        Fetches collections which are active under the given bucket:scope

        :param bucket: Bucket object under which to fetch the collections
        :param scope_name: Scope name under which to fetch the collections
        :param only_names: Boolean to fetch only the collection names
                           instead of collection objects
        """
        collections = list()
        scope = ScopeUtils.get_scope_obj(bucket, scope_name)

        for collection_name, collection in scope.collections.items():
            if not collection.is_dropped:
                if only_names:
                    collections.append(collection_name)
                else:
                    collections.append(collection)
        return collections

    @staticmethod
    def create_collection_object(bucket, scope_name, collection_spec):
        """
        Function to append the newly created collection under the
        scope object of the bucket.

        :param bucket: Bucket object under which the collection is created
        :param scope_name: Scope name under which the collection is created
        :param collection_spec: Collection spec as required by the
                                bucket.Collection class
        """
        collection_name = collection_spec.get("name")
        scope = ScopeUtils.get_scope_obj(bucket, scope_name)
        collection = CollectionUtils.get_collection_obj(scope, collection_name)

        # If collection already dropped with same name use it or create one
        if "history" not in collection_spec.keys():
            collection_spec["history"] = \
                bucket.historyRetentionCollectionDefault \
                    if bucket.storageBackend == Bucket.StorageBackend.magma else "false"
        if collection:
            Collection.recreated(collection, collection_spec)
        else:
            collection = Collection(collection_spec)
            scope.collections[collection_name] = collection

    @staticmethod
    def set_history_retention_for_collection(cluster_node, bucket,
                                             scope, collection, history):
        status, content = BucketHelper(cluster_node).set_collection_history(
            bucket.name, scope, collection, history=history)
        if status:
            bucket.scopes[scope].collections[collection].history = history
        return status, content

    @staticmethod
    def set_maxTTL_for_collection(cluster_node, bucket, scope,
                                  collection, maxttl):
        status, _ = BucketHelper(cluster_node).edit_collection(
            bucket.name, scope, collection, collection_spec={"maxTTL": maxttl})
        if status:
            bucket.scopes[scope].collections[collection].maxTTL = maxttl
        return status

    @staticmethod
    def mark_collection_as_dropped(bucket, scope_name, collection_name):
        """
        Function to mark the collection as dropped

        :param bucket: Bucket object under which the collection is dropped
        :param scope_name: Scope name under which the collection is dropped
        :param collection_name: Collection name to be marked as dropped
        """
        scope = ScopeUtils.get_scope_obj(bucket, scope_name)
        collection = CollectionUtils.get_collection_obj(scope, collection_name)
        collection.is_dropped = True

    @staticmethod
    def mark_collection_as_flushed(bucket, scope_name, collection_name, skip_resetting_num_items=False):
        """
        Function to mark the collection object as flushed
        :param bucket: Bucket object under which the collection is flushed
        :param scope_name: Scope name under which the collection is flushed
        :param collection_name: Collection name to be marked as flushed
        :param skip_resetting_num_items: whether to skip resetting num_items of collection object;
                useful when same number of items need to be reloaded into the collection after
                bucket flush
        :return:
        """
        scope = ScopeUtils.get_scope_obj(bucket, scope_name)
        collection = CollectionUtils.get_collection_obj(scope, collection_name)
        Collection.flushed(collection, skip_resetting_num_items=skip_resetting_num_items)

    @staticmethod
    def create_collection(node, bucket,
                          scope_name=CbServer.default_collection,
                          collection_spec=dict(),
                          session=None):
        """
        Function to create collection under the given scope_name

        :param node: TestInputServer object to create a rest/sdk connection
        :param bucket: Bucket object under which the scope should be created
        :param scope_name: Scope_name under which the collection to be created
        :param collection_spec: Collection spec as expected by
                                bucket.Collection class
        :param session: session to be used instead of httplib2 _http request
        """
        collection_name = collection_spec.get("name")
        CollectionUtils.log.debug("Creating Collection %s:%s:%s"
                                  % (bucket.name, scope_name, collection_name))
        status, content = BucketHelper(node).create_collection(bucket.name,
                                                               scope_name,
                                                               collection_spec,
                                                               session=session)
        if status is False:
            CollectionUtils.log.error(
                "Collection '%s:%s:%s' creation failed: %s"
                % (bucket, scope_name, collection_name, content))
            raise Exception("create_collection failed")
        bucket.stats.increment_manifest_uid()
        CollectionUtils.create_collection_object(bucket,
                                                 scope_name,
                                                 collection_spec)

    @staticmethod
    def drop_collection(node, bucket, scope_name=CbServer.default_scope,
                        collection_name=CbServer.default_collection,
                        session=None):
        """
        Function to drop collection under the given scope_name

        :param node: TestInputServer object to create a rest/sdk connection
        :param bucket: Bucket object under which the scope should be dropped
        :param scope_name: Scope_name under which the collection to be dropped
        :param collection_name: Collection name which has to dropped
        :param session: session to be used instead of httplib2 _http request
        """
        CollectionUtils.log.debug("Dropping Collection %s:%s:%s"
                                  % (bucket.name, scope_name, collection_name))
        status, content = BucketHelper(node).delete_collection(bucket.name,
                                                               scope_name,
                                                               collection_name,
                                                               session=session)
        if status is False:
            CollectionUtils.log.error(
                "Collection '%s:%s:%s' delete failed: %s"
                % (bucket, scope_name, collection_name, content))
            raise Exception("delete_collection")
        bucket.stats.increment_manifest_uid()
        CollectionUtils.mark_collection_as_dropped(bucket,
                                                   scope_name,
                                                   collection_name)

    @staticmethod
    def flush_collection(node, bucket, scope_name=CbServer.default_scope,
                         collection_name=CbServer.default_collection):
        """
        Function to flush collection under the given scope_name

        :param node: TestInputServer object to create a rest/sdk connection
        :param bucket: Bucket object under which the scope should be dropped
        :param scope_name: Scope_name under which the collection to be dropped
        :param collection_name: Collection name which has to dropped
        """
        CollectionUtils.log.debug("Flushing Collection %s:%s:%s"
                                  % (bucket.name, scope_name, collection_name))
        status, content = BucketHelper(node).flush_collection(bucket,
                                                              scope_name,
                                                              collection_name)
        if status is False:
            CollectionUtils.log.error(
                "Collection '%s:%s:%s' flush failed: %s"
                % (bucket, scope_name, collection_name, content))
            raise Exception("delete_collection")

        CollectionUtils.mark_collection_as_flushed(bucket,
                                                   scope_name,
                                                   collection_name)

    @staticmethod
    def get_bucket_template_from_package(module_name):
        spec_package = importlib.import_module(
            'pytests.bucket_collections.bucket_templates.' +
            module_name)
        return copy.deepcopy(spec_package.spec)

    @staticmethod
    def get_crud_template_from_package(module_name):
        spec_package = importlib.import_module(
            'pytests.bucket_collections.collection_ops_specs.' +
            module_name)
        return copy.deepcopy(spec_package.spec)

    @staticmethod
    def create_collections(cluster, bucket, num_collections, scope_name,
                           collection_name=None,
                           create_collection_with_scope_name=False):
        """
        Generic function to create required num_of_collections.

        :param cluster: Cluster object for fetching master/other nodes
        :param bucket:
            Bucket object under which the scopes need
            to be created
        :param num_collections:
            Number of collections to be created under
            the given 'scope_name'
        :param scope_name:
            Scope under which the collections needs to be created
        :param collection_name:
            Generic name to be used for naming a scope.
            'None' results in creating collection with random names
        :param create_collection_with_scope_name:
            Boolean to decide whether to create the first collection
            with the same 'scope_name'
        :return created_collection_names: Dict of all created collection names
        """
        created_collections = 0
        created_collections_dict = dict()
        target_scope = bucket.scopes[scope_name]
        while created_collections < num_collections:
            collection_created = False
            while not collection_created:
                if created_collections == 0 \
                        and create_collection_with_scope_name:
                    col_name = scope_name
                else:
                    if collection_name is None:
                        col_name = BucketUtils.get_random_name(
                            max_length=CbServer.max_collection_name_len)
                    else:
                        col_name = "%s_%s" % (collection_name,
                                              created_collections)

                collection_already_exists = \
                    col_name in target_scope.collections.keys() \
                    and target_scope.collections[col_name].is_dropped is False

                try:
                    BucketUtils.create_collection(cluster.master,
                                                  bucket,
                                                  scope_name,
                                                  {"name": col_name})
                    if collection_already_exists:
                        raise Exception("Collection with duplicate name '%s'"
                                        "got created under bucket:scope "
                                        "%s:%s"
                                        % (col_name, bucket, scope_name))
                    created_collections += 1
                    collection_created = True
                    created_collections_dict[col_name] = dict()
                except Exception as e:
                    if collection_already_exists and collection_name is None:
                        CollectionUtils.log.info(
                            "Collection creation with duplicate "
                            "name '%s' failed as expected. Will "
                            "retry with different name" % col_name)
                    else:
                        CollectionUtils.log.error(
                            "Collection creation failed!")
                        raise Exception(e)
        return created_collections_dict


class ScopeUtils(CollectionUtils):
    @staticmethod
    def get_scope_obj(bucket, scope_name):
        """
        Fetch the target scope object under the given bucket object

        :param bucket: Bucket object to iterate through all scopes
        :param scope_name: Target scope name to fetch the object
        """
        scope = None
        if scope_name in bucket.scopes.keys():
            scope = bucket.scopes[scope_name]
        return scope

    @staticmethod
    def get_active_scopes(bucket, only_names=False):
        """
        Fetches scopes which are active under the given bucket object

        :param bucket: Bucket object under which to fetch the scopes from
        :param only_names: Boolean to enable getting only the scope names
                           instead of objects
        """
        scopes = list()
        for scope_name, scope in bucket.scopes.items():
            if not scope.is_dropped:
                if only_names:
                    scopes.append(scope_name)
                else:
                    scopes.append(scope)
        return scopes

    @staticmethod
    def create_scope_object(bucket, scope_spec=dict()):
        """
        Function to append the newly created scope object under
        the bucket object

        :param bucket: Bucket object under which the collection is created
        :param scope_spec: Scope spec as required by the bucket.Scope class
        """
        scope_name = scope_spec.get("name")
        scope = ScopeUtils.get_scope_obj(bucket, scope_name)

        # If collection already dropped with same name use it or create one
        if scope:
            Scope.recreated(scope, scope_spec)
        else:
            scope = Scope(scope_spec)
            bucket.scopes[scope_name] = scope

    @staticmethod
    def mark_scope_as_dropped(bucket, scope_name):
        """
        Function to mark the scope as dropped.
        Also mark all the collection under the scope as dropped.

        :param bucket: Bucket object under which the scope is dropped
        :param scope_name: Scope name under is marked as dropped
        """
        scope = ScopeUtils.get_scope_obj(bucket, scope_name)

        # Mark all collection under the scope as dropped
        for collection_name, _ in scope.collections.items():
            CollectionUtils.mark_collection_as_dropped(bucket,
                                                       scope.name,
                                                       collection_name)
        # Mark the scope as dropped
        scope.is_dropped = True

    @staticmethod
    def create_scope(node, bucket, scope_spec=dict(), session=None):
        """
        Function to create a scope under the given bucket

        :param node: TestInputServer object to create a rest/sdk connection
        :param bucket: Bucket object under which the scope should be dropped
        :param scope_spec: Scope_spec as expected by the bucket.Scope class
        :param session: session to be used instead of httplib2 _http request
        """
        scope_name = scope_spec.get("name")
        ScopeUtils.log.debug("Creating Scope %s:%s"
                             % (bucket, scope_name))
        status, content = BucketHelper(node).create_scope(
            bucket.name, scope_name, session=session)
        if status is False:
            ScopeUtils.log.error("Scope '%s:%s' creation failed: %s"
                                 % (bucket, scope_name, content))
            raise Exception("create_scope failed")
        # Until MB-44741 is fixed/resolved
        # content = json.loads(content)
        # BucketHelper(node).wait_for_collections_warmup(bucket, content["uid"])
        bucket.stats.increment_manifest_uid()
        ScopeUtils.create_scope_object(bucket, scope_spec)

    @staticmethod
    def drop_scope(node, bucket, scope_name, session=None):
        """
        Function to drop scope under the given bucket

        :param node: TestInputServer object to create a rest/sdk connection
        :param bucket: Bucket object under which the scope should be dropped
        :param scope_name: Scope_name to be dropped
        :param session: session to be used instead of httplib2 _http request
        """
        ScopeUtils.log.debug("Dropping Scope %s:%s"
                             % (bucket, scope_name))
        status, content = BucketHelper(node).delete_scope(bucket.name,
                                                          scope_name,
                                                          session=session)
        if status is False:
            ScopeUtils.log.error("Scope '%s:%s' deletion failed: %s"
                                 % (bucket, scope_name, content))
            raise Exception("delete_scope failed")
        # Until MB-44741 is fixed/resolved
        # content = json.loads(content)
        # BucketHelper(node).wait_for_collections_warmup(bucket, content["uid"])
        bucket.stats.increment_manifest_uid()
        ScopeUtils.mark_scope_as_dropped(bucket, scope_name)

    @staticmethod
    def create_scopes(cluster, bucket, num_scopes, scope_name=None,
                      collection_count=0):
        """
        Generic function to create required num_of_scopes.

        :param cluster: Cluster object for fetching master/other nodes
        :param bucket: Bucket object under which the scopes need
                       to be created
        :param num_scopes: Number of scopes to be created under the 'bucket'
        :param scope_name: Generic name to be used for naming a scope.
                           'None' results in creating random scope names
        :param collection_count: Number for collections to be created
                                 under each created scope
        :return created_scopes_dict: List of all created scope names
        """
        created_scopes = 0
        created_scopes_dict = dict()
        while created_scopes < num_scopes:
            curr_scope_name = "%s_%s" % (scope_name, created_scopes)

            scope_created = False
            while not scope_created:
                if scope_name is None:
                    curr_scope_name = BucketUtils.get_random_name(
                        max_length=CbServer.max_scope_name_len)
                scope_already_exists = \
                    curr_scope_name in bucket.scopes.keys() \
                    and \
                    bucket.scopes[curr_scope_name].is_dropped is False
                try:
                    scope_spec = {"name": curr_scope_name}
                    ScopeUtils.create_scope(cluster.master,
                                            bucket,
                                            scope_spec)
                    if scope_already_exists:
                        raise Exception("Scope with duplicate name is "
                                        "created under bucket %s" % bucket)
                    created_scopes += 1
                    scope_created = True
                    created_scopes_dict[curr_scope_name] = dict()
                    created_scopes_dict[curr_scope_name]["collections"] = \
                        CollectionUtils.create_collections(
                            cluster,
                            bucket,
                            collection_count,
                            curr_scope_name)
                except Exception as e:
                    if scope_already_exists and scope_name is None:
                        ScopeUtils.log.info(
                            "Scope creation with duplicate name "
                            "'%s' failed as expected. Will retry "
                            "with different name")
                    else:
                        ScopeUtils.log.error("Scope creation failed!")
                        raise Exception(e)
        return created_scopes_dict


class BucketUtils(ScopeUtils):
    def __init__(self, cluster_util, server_task):
        self.task = server_task
        self.task_manager = self.task.jython_task_manager
        self.cluster_util = cluster_util
        self.input = TestInputSingleton.input
        self.enable_time_sync = self.input.param("enable_time_sync", False)
        self.sdk_compression = self.input.param("sdk_compression", True)
        self.data_collector = DataCollector()
        self.data_analyzer = DataAnalyzer()
        self.result_analyzer = DataAnalysisResultAnalyzer()
        self.log = ScopeUtils.log

    def assertTrue(self, expr, msg=None):
        if msg:
            msg = "{0} is not true : {1}".format(expr, msg)
        else:
            msg = "{0} is not true".format(expr)
        if not expr:
            raise (Exception(msg))

    def assertFalse(self, expr, msg=None):
        if msg:
            msg = "{0} is not false: {1}".format(expr, msg)
        else:
            msg = "{0} is false".format(expr)
        if expr:
            raise (Exception(msg))

    @staticmethod
    def get_random_name(invalid_name=False,
                        max_length=CbServer.max_bucket_name_len,
                        prefix=None, counter_obj=None):
        """
        API to generate random name which can be used to name
        a bucket/scope/collection
        """
        now = datetime.datetime.now()
        # Passing prefix creates simple names with given counters
        if prefix and counter_obj:
            return "%s-%s" % (prefix, counter_obj.get_next())

        invalid_start_chars = "_%"
        spl_chars = "-"
        invalid_chars = "!@#$^&*()~`:;?/>.,<{}|\]["
        char_set = string.ascii_letters \
                   + string.digits \
                   + invalid_start_chars \
                   + spl_chars
        if invalid_name:
            char_set += invalid_chars

        rand_name = ""
        postfix = str(now.second) + "-" + str(int(round(now.microsecond, 6)))
        name_len = random.randint(1, (max_length - len(postfix) - 1))

        while rand_name == "":
            rand_name = ''.join(random.choice(char_set)
                                for _ in range(name_len))
            if invalid_name:
                if rand_name[0] not in invalid_start_chars and \
                        not set(invalid_chars).intersection(set(rand_name)):
                    rand_name = ""
            # Remove if name starts with invalid_start_charset
            elif rand_name[0] in invalid_start_chars:
                rand_name = ""
        rand_name = rand_name + "-" + postfix
        return rand_name

    @staticmethod
    def get_supported_durability_levels(minimum_level=SDKConstants.DurabilityLevel.NONE):
        """ Returns all the durability levels sorted by relative strength.

        :param minimum_level: The minimum_level e.g. `SDKConstants.DurabilityLevel.None`.
        """
        levels = [SDKConstants.DurabilityLevel.NONE,
                  SDKConstants.DurabilityLevel.MAJORITY,
                  SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                  SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY]

        if minimum_level not in levels:
            raise ValueError("minimum_level must be in {}".format(levels))

        return levels[levels.index(minimum_level):]

    @staticmethod
    def get_random_buckets(buckets, req_num, exclude_from=dict()):
        """
        Function to select random buckets from the given buckets object

        :param buckets: List of bucket objects
        :param req_num: Required number of buckets to be selected in random
                        If req_num is >= the given buckets, all are selected
        :param exclude_from: Dict having bucket names as key to exclude.
                             This is the same dict returned by this function
        :return selected_buckets: Dict with keys as bucket_names
        """
        selected_buckets = dict()
        available_buckets = [bucket.name for bucket in buckets]
        if req_num == "all" or req_num >= len(buckets):
            selected_bucket_names = available_buckets
        else:
            available_buckets = list(set([bucket.name for bucket in buckets])
                                     - set(exclude_from.keys()))
            selected_bucket_names = sample(available_buckets, req_num)

        for bucket_name in selected_bucket_names:
            selected_buckets[bucket_name] = dict()
            selected_buckets[bucket_name]["scopes"] = dict()
        return selected_buckets

    @staticmethod
    def get_random_scopes(buckets, req_num, consider_buckets,
                          avoid_default=False,
                          consider_only_dropped=False,
                          exclude_from=dict()):
        """
        Function to select random scopes from the given buckets object

        :param buckets: List of bucket objects
        :param req_num: Required number of scopes to be selected in random
                        If req_num is >= the given scopes, all are selected
        :param consider_buckets: Required number of buckets to be selected
                                 in random. If req_num is >= the available
                                 buckets, all are considered for selection
        :param avoid_default: (Bool) To avoid selecting default scope.
                              Useful while selecting scopes to drop
        :param consider_only_dropped: Enables selecting dropped scopes
        :param exclude_from: Dict having scope names as key to exclude.
                             This is the same dict returned by this function
        :return selected_buckets: Dict with (key, value) (b_names, scope_dict)
        """
        selected_buckets = BucketUtils.get_random_buckets(buckets,
                                                          consider_buckets)
        available_scopes = list()
        known_buckets = dict()
        exclude_scopes = list()

        for b_name, scope_dict in exclude_from.items():
            for scope_name in list(scope_dict["scopes"].keys()):
                exclude_scopes.append("%s:%s" % (b_name, scope_name))

        for bucket_name in list(selected_buckets.keys()):
            bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
            known_buckets[bucket_name] = bucket
            active_scopes = BucketUtils.get_active_scopes(bucket,
                                                          only_names=True)
            if consider_only_dropped:
                active_scopes = list(set(bucket.scopes.keys())
                                     .difference(set(active_scopes)))
            for scope in active_scopes:
                if scope == CbServer.system_scope \
                        or (scope == CbServer.default_scope and avoid_default):
                    continue
                available_scopes.append("%s:%s" % (bucket_name, scope))

        if req_num == "all" or req_num >= len(available_scopes):
            selected_scopes = list(set(available_scopes)
                                   - set(exclude_scopes))
        else:
            available_scopes = list(set(available_scopes)
                                    - set(exclude_scopes))
            selected_scopes = sample(available_scopes, req_num)

        for scope_name in selected_scopes:
            scope_name = scope_name.split(":")
            bucket_name = scope_name[0]
            scope_name = scope_name[1]
            selected_buckets[bucket_name]["scopes"][scope_name] = dict()
            selected_buckets[bucket_name][
                "scopes"][scope_name][
                "collections"] = dict()
        return selected_buckets

    @staticmethod
    def get_random_collections(buckets, req_num,
                               consider_scopes,
                               consider_buckets,
                               consider_only_dropped=False,
                               exclude_from=dict()):
        """
        Function to select random collections from the given buckets object

        :param buckets: List of bucket objects
        :param req_num: Required number of collections to be selected
                        in random. If req_num is >= the available collections,
                        all are selected
        :param consider_scopes: Required number of scopes to be selected
                                in random. If req_num is >= the given scopes,
                                all are selected
        :param consider_buckets: Required number of buckets to be selected
                                 in random. If req_num is >= the available
                                 buckets, all are selecteed
        :param consider_only_dropped: Enables selecting dropped collections
        :param exclude_from: Dict having scope names as key to exclude.
                             This is the same dict returned by this function
        :return selected_buckets: Dict with (key, value) (b_names, scope_dict)
        """
        selected_buckets = \
            BucketUtils.get_random_scopes(buckets,
                                          consider_scopes,
                                          consider_buckets)
        available_collections = list()
        exclude_collections = list()

        for b_name, scope_dict in exclude_from.items():
            if "scopes" in scope_dict:
                for scope_name, collection_dict in scope_dict["scopes"].items():
                    if "collections" in collection_dict:
                        for c_name in list(collection_dict["collections"].keys()):
                            exclude_collections.append("%s:%s:%s"
                                                       % (b_name, scope_name,
                                                           c_name))

        for bucket_name, scope_dict in selected_buckets.items():
            bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
            for scope_name in list(scope_dict["scopes"].keys()):
                active_collections = BucketUtils.get_active_collections(
                    bucket, scope_name, only_names=True)
                if consider_only_dropped:
                    active_collections = \
                        list(set(bucket.scopes[scope_name].collections.keys())
                             .difference(set(active_collections)))
                for collection in active_collections:
                    available_collections.append("%s:%s:%s"
                                                 % (bucket_name,
                                                    scope_name,
                                                    collection))

        if req_num == "all" or req_num >= len(available_collections):
            selected_collections = list(set(available_collections)
                                        - set(exclude_collections))
        else:
            available_collections = list(set(available_collections)
                                         - set(exclude_collections))
            selected_collections = sample(available_collections, req_num)

        for collection in selected_collections:
            collection_name = collection.split(":")
            bucket_name = collection_name[0]
            scope_name = collection_name[1]
            collection_name = collection_name[2]

            selected_buckets[bucket_name][
                "scopes"][scope_name][
                "collections"][collection_name] = dict()
        return selected_buckets

    # Fetch/Create/Delete buckets
    def load_sample_bucket(self, cluster, sample_bucket):
        bucket = None
        rest = BucketRestApi(cluster.master)
        status, content = rest.load_sample_bucket([sample_bucket.name])
        if not status:
            self.log.critical(f"Sample bucket load failed: {content}")
            return status
        sleep(5, "Wait before fetching buckets from cluster")
        buckets = self.get_all_buckets(cluster)
        for bucket in buckets:
            if bucket.name == sample_bucket.name:
                # Append loaded sample bucket into buckets object list
                cluster.buckets.append(bucket)
                break
        if status:
            self.get_updated_bucket_server_list(cluster, bucket)
            warmed_up = self._wait_warmup_completed(bucket, wait_time=300,
                                                    check_for_persistence=False)
            if not warmed_up:
                status = False
        if status:
            status = False
            retry_count = 600
            sleep_time = 5
            while retry_count > 0:
                item_count = self.get_buckets_item_count(cluster,
                                                         sample_bucket.name)
                if item_count == sample_bucket.stats.expected_item_count:
                    status = True
                    break
                sleep(sleep_time, "Sample bucket still loading")
                retry_count -= sleep_time
        if status is False:
            self.log.error("Sample bucket failed to load the target items")
        return status

    def async_create_bucket(self, cluster, bucket):
        if not isinstance(bucket, Bucket):
            raise Exception("Create bucket needs Bucket object as parameter")
        self.log.debug("Creating bucket: %s" % bucket.name)
        task = BucketCreateTask(cluster.master, bucket)
        self.task_manager.add_new_task(task)
        return task

    def async_create_database(self, cluster, bucket, tenant=None, dataplane_id="", timeout=600):
        if not isinstance(bucket, Bucket):
            raise Exception("Create bucket needs Bucket object as parameter")
        self.log.debug("Creating bucket: %s" % bucket.name)
        task = DatabaseCreateTask(cluster, bucket,
                                  tenant=tenant,
                                  dataplane_id=dataplane_id,
                                  timeout=timeout)
        self.task_manager.add_new_task(task)
        return task

    def async_monitor_database_scaling(self, tracking_list, timeout=300,
                                       ignore_undesired_updates=False):
        self.log.debug("Monitoring scaling for db %s"
                       % [tracker["bucket"].name for tracker in tracking_list])
        task = MonitorServerlessDatabaseScaling(tracking_list, timeout,
                                                ignore_undesired_updates)
        self.task_manager.add_new_task(task)
        return task

    def create_bucket(self, cluster, bucket, wait_for_warmup=True):
        raise_exception = None
        task = self.async_create_bucket(cluster, bucket)
        self.task_manager.get_task_result(task)
        if task.result is False:
            raise_exception = "BucketCreateTask failed"
            raise Exception(raise_exception)

        # Update server_objects with the bucket object for future reference
        self.get_updated_bucket_server_list(cluster, bucket)

        if task.result and wait_for_warmup:
            self.log.debug("Wait for memcached to accept cbstats "
                           "or any other bucket request connections")
            sleep(2)
            warmed_up = self._wait_warmup_completed(bucket, wait_time=300)
            if not warmed_up:
                task.result = False
                raise_exception = "Bucket not warmed up"

        if task.result:
            cluster.buckets.append(bucket)
            if cluster.type != "serverless":
                self.get_all_buckets(cluster)

        self.task_manager.stop_task(task)
        if raise_exception:
            raise_exception = "Create bucket %s failed: %s" \
                              % (bucket.name, raise_exception)
            raise Exception(raise_exception)

    def pause_database(self, cluster, database_id, tenant=None, timeout_to_complete=1800,
                       timeout_to_start=10):
        cloud_hibernation_task = CloudHibernationTask(cluster, database_id, "pause",
                                                      tenant, timeout_to_complete,
                                                      timeout_to_start)
        self.task_manager.add_new_task(cloud_hibernation_task)
        return cloud_hibernation_task

    def resume_database(self, cluster, database_id, tenant=None, timeout_to_complete=1800,
                        timeout_to_start=10):
        cloud_hibernation_task = CloudHibernationTask(cluster, database_id, "resume",
                                                      tenant, timeout_to_complete,
                                                      timeout_to_start)
        self.task_manager.add_new_task(cloud_hibernation_task)
        return cloud_hibernation_task

    def pause_bucket(self, cluster, bucket, s3_path, region, rate_limit,
                     timeout, wait_for_bucket_deletion=True):

        self.log.debug('Pausing existing bucket {0} '.format(bucket.name))
        hibernation_task = HibernationTask(cluster.master, bucket.name, s3_path,
                                           region, rate_limit, 'pause', timeout)
        self.task_manager.add_new_task(hibernation_task)
        if wait_for_bucket_deletion:
            self.task_manager.get_task_result(hibernation_task)
            return hibernation_task
        return hibernation_task

    def stop_pause(self, cluster, bucket):
        self.log.debug('Stoping pause for bucket {0}'.format(bucket.name))
        bucket_conn = BucketHelper(cluster.master)
        rest = RestConnection(cluster.master)
        status, content = bucket_conn.stop_pause(bucket.name)
        result = True
        if not status:
            result = False
            return result, content
        pause_task = rest.ns_server_tasks(task_type="hibernation")
        if not pause_task:
            result = False
        else:
            status = pause_task['status']
            if not (status == 'stopped'):
                result = False
        return result, content

    def resume_bucket(self, cluster, bucket, s3_path, region, rate_limit,
                      timeout, wait_for_bucket_creation=True):

        self.log.debug('Resuming bucket {0} '.format(bucket.name))
        hibernation_task = HibernationTask(cluster.master, bucket.name, s3_path,
                                           region, rate_limit, 'resume', timeout)
        self.task_manager.add_new_task(hibernation_task)
        if wait_for_bucket_creation:
            self.task_manager.get_task_result(hibernation_task)
            return hibernation_task
        return hibernation_task

    def stop_resume(self, cluster, bucket):
        self.log.debug('Stoping pause for bucket {0}'.format(bucket.name))
        bucket_conn = BucketHelper(cluster.master)
        rest = RestConnection(cluster.master)
        status, content = bucket_conn.stop_resume(bucket.name)
        result = True
        if not status:
            result = False
            return result, content
        pause_task = rest.ns_server_tasks(task_type="hibernation")
        if not pause_task:
            result = False
        else:
            status = pause_task['status']
            if not (status == 'stopped'):
                result = False
        return result, content

    def delete_bucket(self, cluster, bucket, wait_for_bucket_deletion=True):
        self.log.debug('Deleting existing bucket {0} on {1}'
                       .format(bucket, cluster.master))
        bucket_rest = BucketRestApi(cluster.master)
        if self.bucket_exists(cluster, bucket):
            status, _ = bucket_rest.delete_bucket(bucket.name)
            if not status:
                self.log.critical("Delete bucket failed")
            else:
                # Pop bucket object from cluster.buckets
                for index, t_bucket in enumerate(cluster.buckets):
                    if t_bucket.name == bucket.name:
                        cluster.buckets.pop(index)

            self.log.debug('Deleted bucket: {0} from {1}'
                           .format(bucket, cluster.master.ip))
        if wait_for_bucket_deletion:
            if not self.wait_for_bucket_deletion(cluster, bucket, 200):
                return False
            return True

    def wait_for_bucket_deletion(self, cluster, bucket,
                                 timeout_in_seconds=120):
        self.log.debug("Waiting for bucket %s deletion to finish"
                       % bucket.name)
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if not self.bucket_exists(cluster, bucket):
                return True
            else:
                sleep(2)
        return False

    def wait_for_bucket_creation(self, cluster, bucket,
                                 timeout_in_seconds=120):
        self.log.debug('Waiting for bucket creation to complete')
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if self.bucket_exists(cluster, bucket):
                return True
            else:
                sleep(2)
        return False

    def check_compaction_status(self, server_node, bucket_name):
        rest = ClusterRestAPI(server_node)
        status, tasks = rest.cluster_tasks()
        if "error" in tasks:
            raise Exception(tasks)
        for task in tasks:
            self.log.debug("Task is {0}".format(task))
            if task["type"] == "bucket_compaction":
                if task["bucket"] == bucket_name:
                    return True, task["progress"]
        return False, None

    def wait_till_compaction_end(self, server_node, bucket_name,
                                 timeout_in_seconds=60):
        end_time = time.time() + float(timeout_in_seconds)
        while time.time() < end_time:
            status, progress = self.check_compaction_status(
                server_node, bucket_name)
            if status:
                sleep(1, "Compaction in progress - %s%%" % progress,
                      log_type="infra")
            else:
                # the compaction task has completed
                return True

        self.log.error("Auto compaction has not ended in {0} sec."
                       .format(str(timeout_in_seconds)))
        return False

    def bucket_exists(self, cluster, bucket):
        for item in self.get_all_buckets(cluster):
            if item.name == bucket.name:
                return True
        return False

    def delete_all_buckets(self, cluster):
        for server in cluster.servers:
            buckets = self.get_all_buckets(cluster, cluster_node=server)
            for bucket in buckets:
                self.log.debug("%s - Remove bucket %s"
                               % (server.ip, bucket.name))
                try:
                    status = self.delete_bucket(cluster, bucket)
                except Exception as e:
                    self.log.error(e)
                    raise e
                if not status:
                    raise Exception("%s - Bucket %s could not be deleted"
                                    % (server.ip, bucket.name))

    def create_default_bucket(
            self, cluster, bucket_type=Bucket.Type.MEMBASE,
            ram_quota=None, replica=1, maxTTL=0,
            compression_mode="off", wait_for_warmup=True,
            conflict_resolution=Bucket.ConflictResolution.SEQ_NO,
            replica_index=1,
            bucket_rank=None,
            bucket_priority=Bucket.Priority.LOW,
            storage=Bucket.StorageBackend.magma,
            eviction_policy=None,
            flush_enabled=Bucket.FlushBucket.DISABLED,
            bucket_durability=Bucket.DurabilityMinLevel.NONE,
            purge_interval=1,
            autoCompactionDefined="false",
            fragmentation_percentage=50,
            bucket_name="default",
            history_retention_collection_default="true",
            history_retention_bytes=0,
            history_retention_seconds=0,
            magma_key_tree_data_block_size=4096,
            magma_seq_tree_data_block_size=4096,
            vbuckets=None, weight=None, width=None,
            durability_impossible_fallback=None,
            warmup_behavior=Bucket.WarmupBehavior.BACKGROUND,
            fusion_log_store_uri=None):
        node_info = global_vars.cluster_util.get_nodes_self(cluster.master)
        if ram_quota:
            ram_quota_mb = ram_quota
        elif node_info.memoryQuota and int(node_info.memoryQuota) > 0:
            ram_available = node_info.memoryQuota
            ram_quota_mb = ram_available - 1
        if autoCompactionDefined == "true":
            compression_mode = "passive"

        if eviction_policy is None:
            if storage == Bucket.StorageBackend.magma:
                eviction_policy = Bucket.EvictionPolicy.FULL_EVICTION
            else:
                eviction_policy = Bucket.EvictionPolicy.VALUE_ONLY

        bucket_obj = Bucket(
            {Bucket.name: bucket_name,
             Bucket.bucketType: bucket_type,
             Bucket.ramQuotaMB: ram_quota_mb,
             Bucket.replicaNumber: replica,
             Bucket.compressionMode: compression_mode,
             Bucket.maxTTL: maxTTL,
             Bucket.priority: bucket_priority,
             Bucket.conflictResolutionType: conflict_resolution,
             Bucket.replicaIndex: replica_index,
             Bucket.storageBackend: storage,
             Bucket.rank: bucket_rank,
             Bucket.evictionPolicy: eviction_policy,
             Bucket.flushEnabled: flush_enabled,
             Bucket.durabilityMinLevel: bucket_durability,
             Bucket.purge_interval: purge_interval,
             Bucket.autoCompactionDefined: autoCompactionDefined,
             Bucket.fragmentationPercentage: fragmentation_percentage,
             Bucket.historyRetentionCollectionDefault: history_retention_collection_default,
             Bucket.historyRetentionSeconds: history_retention_seconds,
             Bucket.historyRetentionBytes: history_retention_bytes,
             Bucket.magmaKeyTreeDataBlockSize: magma_key_tree_data_block_size,
             Bucket.magmaSeqTreeDataBlockSize: magma_seq_tree_data_block_size,
             Bucket.numVBuckets: vbuckets,
             Bucket.width: width,
             Bucket.weight: weight,
             Bucket.durabilityImpossibleFallback: durability_impossible_fallback,
             Bucket.warmupBehavior: warmup_behavior})

        if fusion_log_store_uri is not None:
            bucket_obj.fusionLogstoreURI = fusion_log_store_uri

        if cluster.type == "dedicated":
            bucket_params = {
                CloudCluster.Bucket.name: bucket_obj.name,
                CloudCluster.Bucket.conflictResolutionType:
                    conflict_resolution,
                CloudCluster.Bucket.ramQuotaMB: bucket_obj.ramQuotaMB,
                CloudCluster.Bucket.flushEnabled:
                    True if bucket_obj.flushEnabled else False,
                CloudCluster.Bucket.replicaNumber: bucket_obj.replicaNumber,
                CloudCluster.Bucket.durabilityMinLevel:
                    bucket_obj.durabilityMinLevel,
                CloudCluster.Bucket.maxTTL:
                    {"unit": "seconds", "value": bucket_obj.maxTTL}
            }
            DedicatedCapellaUtils.create_bucket(cluster, bucket_params)
            cluster.buckets.append(bucket_obj)
        elif cluster.type == "serverless":
            task = self.async_create_database(cluster, bucket_obj)
            self.task_manager.get_task_result(task)
        else:
            bucket_obj.historyRetentionCollectionDefault = history_retention_collection_default
            bucket_obj.historyRetentionSeconds = history_retention_seconds
            bucket_obj.historyRetentionBytes = history_retention_bytes
            bucket_obj.magmaKeyTreeDataBlockSize = magma_key_tree_data_block_size
            bucket_obj.magmaSeqTreeDataBlockSize = magma_seq_tree_data_block_size
            self.create_bucket(cluster, bucket_obj, wait_for_warmup)
            if self.enable_time_sync:
                self._set_time_sync_on_buckets(cluster, [bucket_obj.name])

    def get_updated_bucket_server_list(self, cluster, bucket_obj):
        retry = 15
        helper = BucketHelper(cluster.master)
        self.log.debug("Updating server_list for bucket :: %s"
                       % bucket_obj.name)
        while retry > 0:
            # Reset the known servers list
            bucket_obj.servers = list()
            try:
                b_stat = helper.get_buckets_json(bucket_obj.name)
                self.log.debug("%s - Server list::%s"
                               % (bucket_obj.name,
                                  b_stat["vBucketServerMap"]["serverList"]))
                for server in b_stat["vBucketServerMap"]["serverList"]:
                    ip, mc_port = server.split(":")
                    mc_port = int(mc_port)
                    for node in cluster.nodes_in_cluster:
                        found = False
                        if node.ip == ip:
                            if ClusterRun.is_enabled:
                                if CbServer.use_https:
                                    n_index = (ClusterRun.ssl_memcached_port
                                               - node.memcached_port) / 4
                                    if mc_port == ClusterRun.memcached_port \
                                            + (2 * n_index):
                                        found = True
                                else:
                                    if node.memcached_port == mc_port:
                                        found = True
                            else:
                                found = True
                            if found:
                                bucket_obj.servers.append(node)
                                break
            except GetBucketInfoFailed:
                pass
            finally:
                if bucket_obj.servers:
                    break
                retry -= 1
            if not bucket_obj.servers:
                sleep(2, "No servers_list found, Will retry...")
        self.log.debug("Bucket %s occupies servers: %s"
                       % (bucket_obj.name, bucket_obj.servers))
        cluster.bucketNodes[bucket_obj] = bucket_obj.servers

    @staticmethod
    def update_bucket_nebula_servers(cluster, bucket_obj):
        nebula_servers = list()
        nebula = bucket_obj.serverless.nebula_obj
        b_stats = BucketHelper(nebula.endpoint)\
            .get_buckets_json(bucket_obj.name)
        for server in b_stats["nodes"]:
            nebula_servers.append(nebula.servers[server["hostname"]])
        cluster.bucketDNNodes[bucket_obj] = nebula_servers
        if not bucket_obj.servers:
            bucket_obj.servers = nebula_servers
            cluster.bucketNodes[bucket_obj] = nebula_servers

    @staticmethod
    def expand_collection_spec(buckets_spec, bucket_name, scope_name):
        scope_spec = \
            buckets_spec["buckets"][bucket_name]["scopes"][scope_name]

        def_num_docs = \
            buckets_spec["buckets"][bucket_name].get(
                MetaConstants.NUM_ITEMS_PER_COLLECTION, 0)

        req_num_collections = \
            scope_spec.get(
                MetaConstants.NUM_COLLECTIONS_PER_SCOPE,
                buckets_spec["buckets"][bucket_name].get(
                    MetaConstants.NUM_COLLECTIONS_PER_SCOPE, 0))
        def_num_docs = scope_spec.get(MetaConstants.NUM_ITEMS_PER_COLLECTION,
                                      def_num_docs)

        # Create default collection under default scope if required
        if scope_name == CbServer.default_scope and (
                not scope_spec.get(MetaConstants.REMOVE_DEFAULT_COLLECTION,
                                   False)):
            scope_spec["collections"][CbServer.default_collection] = dict()

        # Created extra collection specs which are not provided in spec
        collection_obj_index = len(scope_spec["collections"])
        prefix = counter_obj = None
        if buckets_spec[MetaConstants.USE_SIMPLE_NAMES]:
            prefix = "collection"
            counter_obj = Bucket.collection_counter
        while collection_obj_index < req_num_collections:
            collection_name = BucketUtils.get_random_name(
                max_length=CbServer.max_collection_name_len,
                prefix=prefix, counter_obj=counter_obj)
            if collection_name in scope_spec["collections"].keys():
                continue
            scope_spec["collections"][collection_name] = dict()
            collection_obj_index += 1

        # Expand spec values within all collection definition specs
        for _, collection_spec in scope_spec["collections"].items():
            if MetaConstants.NUM_ITEMS_PER_COLLECTION not in collection_spec:
                collection_spec[MetaConstants.NUM_ITEMS_PER_COLLECTION] = \
                    def_num_docs

    @staticmethod
    def expand_scope_spec(buckets_spec, bucket_name):
        scope_spec = buckets_spec["buckets"][bucket_name]["scopes"]

        # Create default scope object under scope_specs
        if CbServer.default_scope not in scope_spec.keys():
            scope_spec[CbServer.default_scope] = dict()
            scope_spec[CbServer.default_scope]["collections"] = dict()

        def_remove_default_collection = \
            buckets_spec[MetaConstants.REMOVE_DEFAULT_COLLECTION]

        req_num_scopes = buckets_spec["buckets"][
            bucket_name].get(MetaConstants.NUM_SCOPES_PER_BUCKET,
                             buckets_spec[MetaConstants.NUM_SCOPES_PER_BUCKET])

        # Create required Scope specs which are not provided by user
        for scope_name, spec_val in scope_spec.items():
            if type(spec_val) is not dict:
                continue

            if MetaConstants.REMOVE_DEFAULT_COLLECTION not in spec_val:
                spec_val[MetaConstants.REMOVE_DEFAULT_COLLECTION] = \
                    def_remove_default_collection

            BucketUtils.expand_collection_spec(buckets_spec,
                                               bucket_name,
                                               scope_name)

        scope_obj_index = len(scope_spec)
        prefix = counter_obj = None
        if buckets_spec[MetaConstants.USE_SIMPLE_NAMES]:
            prefix = "scope"
            counter_obj = Bucket.scope_counter
        while scope_obj_index < req_num_scopes:
            scope_name = BucketUtils.get_random_name(
                max_length=CbServer.max_scope_name_len,
                prefix=prefix, counter_obj=counter_obj)
            if scope_name in scope_spec.keys():
                continue

            scope_spec[scope_name] = dict()
            scope_spec[scope_name]["collections"] = dict()

            # Expand scopes further within existing bucket definition
            BucketUtils.expand_collection_spec(buckets_spec,
                                               bucket_name,
                                               scope_name)
            scope_obj_index += 1

    @staticmethod
    def expand_buckets_spec(rest_conn, buckets_spec):
        total_ram_requested_explicitly = 0

        # Calculate total RAM explicitly requested in bucket specs
        if "buckets" not in buckets_spec:
            buckets_spec["buckets"] = dict()

        # Calculate total_ram_requested by spec explicitly
        for _, bucket_spec in buckets_spec["buckets"].items():
            if Bucket.ramQuotaMB in bucket_spec:
                total_ram_requested_explicitly += \
                    bucket_spec[Bucket.ramQuotaMB]

        req_num_buckets = buckets_spec.get(MetaConstants.NUM_BUCKETS,
                                           len(buckets_spec["buckets"]))

        # Fetch and define RAM quota for buckets
        if Bucket.ramQuotaMB not in buckets_spec:
            _, node_info = rest_conn.node_details()
            if node_info["memoryQuota"] and int(node_info["memoryQuota"]) > 0:
                ram_available = node_info["memoryQuota"]
                ram_available -= total_ram_requested_explicitly
                buckets_spec[Bucket.ramQuotaMB] = \
                    int(ram_available / req_num_buckets)

        # Construct default values to use for all bucket objects
        bucket_defaults = dict()
        bucket_level_meta = MetaConstants.get_params()
        bucket_level_meta.remove(MetaConstants.NUM_BUCKETS)
        for param_name in bucket_level_meta:
            if param_name in buckets_spec:
                bucket_defaults[param_name] = buckets_spec[param_name]
            else:
                bucket_defaults[param_name] = 0

        bucket_level_defaults = Bucket.get_params()
        for param_name in bucket_level_defaults:
            if param_name in buckets_spec:
                bucket_defaults[param_name] = buckets_spec[param_name]

        # Create required Bucket specs which are not provided by spec
        for bucket_name, bucket_spec in buckets_spec["buckets"].items():
            for param_name, param_value in bucket_defaults.items():
                if param_name not in bucket_spec:
                    bucket_spec[param_name] = param_value

            if "scopes" not in bucket_spec:
                bucket_spec["scopes"] = dict()
            # Expand scopes further within existing bucket definition
            BucketUtils.expand_scope_spec(buckets_spec, bucket_name)

        bucket_obj_index = len(buckets_spec["buckets"])
        prefix = counter_obj = None
        if buckets_spec[MetaConstants.USE_SIMPLE_NAMES]:
            prefix = "bucket"
            counter_obj = Bucket.bucket_counter
        while bucket_obj_index < req_num_buckets:
            bucket_name = BucketUtils.get_random_name(
                prefix=prefix, counter_obj=counter_obj)
            if bucket_name in buckets_spec["buckets"]:
                continue

            buckets_spec["buckets"][bucket_name] = dict()
            buckets_spec["buckets"][bucket_name]["scopes"] = dict()
            for param_name, param_value in bucket_defaults.items():
                buckets_spec["buckets"][bucket_name][param_name] = param_value

            if Bucket.rank not in bucket_defaults:
                buckets_spec["buckets"][bucket_name][Bucket.rank] = \
                    randint(0, 1000)

            # Expand scopes further within created bucket definition
            BucketUtils.expand_scope_spec(buckets_spec, bucket_name)
            bucket_obj_index += 1

        # Handling code for exponential load
        for bucket_name, bucket_spec in buckets_spec["buckets"].items():
            if bucket_spec.get(MetaConstants.LOAD_COLLECTIONS_EXPONENTIALLY,
                               False):
                remaining_num_items = 0
                # Fetch total_items to be expected as the end of initialization
                for s_name, s_spec in bucket_spec["scopes"].items():
                    for c_name, c_spec in s_spec["collections"].items():
                        remaining_num_items += c_spec["num_items"]

                # Set exponential num_items across the collections
                for s_name, s_spec in bucket_spec["scopes"].items():
                    for c_name, c_spec in s_spec["collections"].items():
                        n_items = int(remaining_num_items / 2)
                        if n_items == 0 and remaining_num_items != 0:
                            n_items = remaining_num_items
                            remaining_num_items = 0
                        else:
                            remaining_num_items -= n_items
                        c_spec["num_items"] = n_items

        return buckets_spec["buckets"]

    def create_bucket_from_dict_spec(self, cluster, bucket_name,
                                     bucket_spec, async_create=True):
        task = BucketCreateFromSpecTask(
            self.task_manager, self.cluster_util.get_kv_nodes(cluster),
            bucket_name, bucket_spec)
        self.task_manager.add_new_task(task)
        if not async_create:
            self.task_manager.get_task_result(task)
        return task

    def specs_for_serverless(self, bucket_spec):
        self.balance_scopes_collections_items(bucket_spec)
        if "buckets" in bucket_spec:
            for bucket in bucket_spec["buckets"]:
                self.balance_scopes_collections_items(
                    bucket_spec["buckets"][bucket], bucket_spec)


    def balance_scopes_collections_items(self, bucket_spec, default_spec=None):
        def get_divisor(max_limits_variable):
            factor_list = []
            i = 1
            while i <= math.sqrt(max_limits_variable):
                if max_limits_variable % i == 0:
                    factor_list.append(i)
                i = i + 1
            return_index = (len(factor_list) // 2)
            return factor_list[return_index]

        def bucket_spec_check(spec_name):
            if spec_name not in bucket_spec:
                bucket_spec[spec_name] = default_spec[spec_name]

        if default_spec:
            bucket_spec_check(MetaConstants.NUM_SCOPES_PER_BUCKET)
            bucket_spec_check(MetaConstants.NUM_ITEMS_PER_COLLECTION)
            bucket_spec_check(MetaConstants.NUM_COLLECTIONS_PER_SCOPE)

        max_limits = self.input.param("max_limits", 80)
        if max_limits >= 100 or max_limits <= 0:
            max_limits = 80

        new_collection_per_scope_number = None
        new_scope_number = None
        if (bucket_spec[MetaConstants.NUM_SCOPES_PER_BUCKET] *
            bucket_spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE]) > \
                max_limits:

            # scope and collections limits according to percentage_max_limits
            new_collection_per_scope_number = get_divisor(max_limits)
            new_scope_number = (max_limits
                                / new_collection_per_scope_number)

            # setting new number_items for bucket
            bucket_spec[MetaConstants.NUM_ITEMS_PER_COLLECTION] = \
                int(math.ceil((
                    bucket_spec[MetaConstants.NUM_ITEMS_PER_COLLECTION] *
                    bucket_spec[MetaConstants.NUM_SCOPES_PER_BUCKET] *
                    bucket_spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE]) /
                   (new_collection_per_scope_number * new_scope_number)))

            bucket_spec[MetaConstants.NUM_COLLECTIONS_PER_SCOPE] = \
                new_collection_per_scope_number
            bucket_spec[MetaConstants.NUM_SCOPES_PER_BUCKET] = \
                new_scope_number

    def create_buckets_using_json_data(self, cluster, buckets_spec,
                                       async_create=True, timeout=600,
                                       dataplane_id=""):
        result = True
        self.log.info("Creating required buckets from template")
        load_data_from_existing_tar = False
        bucket_creation_tasks = list()
        bucket_names_ref = dict()
        if cluster.type == "serverless":
            # On Cloud - serverless deployment
            buckets_spec[Bucket.ramQuotaMB] = 256
            buckets_spec = BucketUtils.expand_buckets_spec(None, buckets_spec)

            sleep_time = min(15, len(buckets_spec.items()))
            for bucket_name, bucket_spec in buckets_spec.items():
                # due to AV-49460 need to add some gap in bucket threads
                sleep(sleep_time, "Adding gap between bucket threads creation")
                b_obj = Bucket({
                    Bucket.name: bucket_name,
                    Bucket.bucketType: Bucket.Type.MEMBASE,
                    Bucket.ramQuotaMB: bucket_spec[Bucket.ramQuotaMB],
                    Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
                    Bucket.storageBackend: Bucket.StorageBackend.magma,
                    Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
                    Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
                    Bucket.width: bucket_spec[Bucket.width],
                    Bucket.weight: bucket_spec[Bucket.weight]})
                if Bucket.numVBuckets in bucket_spec:
                    b_obj.numVBuckets = bucket_spec[Bucket.numVBuckets]
                if Bucket.warmupBehavior in bucket_spec:
                    b_obj.warmupBehavior = bucket_spec[Bucket.warmupBehavior]
                if Bucket.fusionLogstoreURI in bucket_spec:
                    b_obj.fusionLogstoreURI = bucket_spec[Bucket.fusionLogstoreURI]
                task = self.async_create_database(cluster, b_obj,
                                                  timeout=timeout,
                                                  dataplane_id=dataplane_id)
                bucket_creation_tasks.append(task)
                bucket_names_ref[task.thread_name] = bucket_name
        else:
            # On Prem case
            rest_conn = ClusterRestAPI(cluster.master)
            if CbServer.cluster_profile == "serverless":
                self.specs_for_serverless(buckets_spec)
            buckets_spec = BucketUtils.expand_buckets_spec(
                rest_conn, buckets_spec)

            for bucket_name, bucket_spec in buckets_spec.items():
                if bucket_spec[MetaConstants.BUCKET_TAR_SRC]:
                    load_data_from_existing_tar = True
                bucket_creation_tasks.append(
                    self.create_bucket_from_dict_spec(
                        cluster, bucket_name, bucket_spec,
                        async_create=async_create))

        for task in bucket_creation_tasks:
            self.task_manager.get_task_result(task)
            if task.result is False:
                result = False
                self.log.error("Failure in bucket creation task: %s"
                               % task.thread_name)
            else:
                if cluster.type == "serverless":
                    spec = buckets_spec.get(bucket_names_ref[task.thread_name])
                    buckets_spec[task.bucket_obj.name] = spec
                    buckets_spec.pop(bucket_names_ref[task.thread_name])
                else:
                    cluster.buckets.append(task.bucket_obj)

        if cluster.type != "serverless":
            self.get_all_buckets(cluster)

        for bucket in cluster.buckets:
            if cluster.type != "serverless":
                self.get_updated_bucket_server_list(cluster, bucket)
            if not buckets_spec.get(bucket.name):
                continue
            for scope_name, scope_spec \
                    in buckets_spec[bucket.name]["scopes"].items():
                if type(scope_spec) is not dict:
                    continue
                scope_spec["name"] = scope_name
                if scope_name != CbServer.default_scope:
                    if cluster.type == "serverless":
                        self.create_scope(bucket.servers[0], bucket,
                                          scope_spec)
                    else:
                        self.create_scope_object(bucket, scope_spec)

                for c_name, c_spec in scope_spec["collections"].items():
                    if type(c_spec) is not dict:
                        continue
                    c_spec["name"] = c_name
                    c_spec["history"] = "false"
                    if bucket.storageBackend == Bucket.StorageBackend.magma:
                        c_spec["history"] = bucket.historyRetentionCollectionDefault
                    if cluster.type == "serverless" \
                            and c_name != CbServer.default_collection:
                        self.create_collection(bucket.servers[0], bucket,
                                               scope_name, c_spec)
                    else:
                        self.create_collection_object(bucket, scope_name, c_spec)

        if load_data_from_existing_tar:
            for bucket_name, bucket_spec in buckets_spec.items():
                bucket_obj = self.get_bucket_obj(cluster.buckets, bucket_name)
                self.load_bucket_from_tar(
                    cluster.nodes_in_cluster, bucket_obj,
                    bucket_spec[MetaConstants.BUCKET_TAR_SRC],
                    bucket_spec[MetaConstants.BUCKET_TAR_DIR])

                # Update new num_items for each collection post data loading
                collection_item_count = \
                    self.get_doc_count_per_collection(cluster, bucket_obj)
                for s_name, scope in bucket_obj.scopes.items():
                    for c_name, collection in scope.collections.items():
                        collection.num_items = \
                            collection_item_count[s_name][c_name]["items"]
        return result

    def load_bucket_from_tar(self, kv_nodes, bucket_obj, tar_src, tar_dir):
        """
        1. Get data storage path info using REST
        2. Stop Couchbase server
        3. Remove old content from the current bucket
        4. Copy and extract the bucket tar into the storage path
        5. Change dir permissions to couchbase:couchbase
        6. Start Couchbase server

        :param kv_nodes: List of kv_nodes in the cluster to load data
        :param bucket_obj: Bucket_obj for which to override data from files
        :param tar_src: Source of the tar files. (remote / local)
        :param tar_dir: Target path from where the files are loaded.
          Examples,
          tar_src=local  - /data/4_nodes_1_replica_10G
          tar_src=remote - 172.23.10.12:/data/4_nodes_1_replica_10G

        """

        def load_bucket_on_node(n_index, shell_conn, b_name, data_path,
                                s_tar_dir):
            file_path = os.path.join(s_tar_dir, "node%d.tar.gz" % n_index)
            remote_tar_file_path = "/tmp/node%d.tar.gz" % n_index
            bucket_path = os.path.join(data_path, b_name)

            BucketUtils.log.info("%s - Loading bucket from tar"
                                 % shell_conn.ip)
            shell_conn.execute_command("rm -rf %s" % remote_tar_file_path)
            shell_conn.copy_file_local_to_remote(file_path,
                                                 remote_tar_file_path)
            shell_conn.execute_command(
                "rm -rf {0}/* ; cd {0} ; tar -zxf {1} ; cd - ; rm -f {1}"
                .format(bucket_path, remote_tar_file_path))
            shell_conn.execute_command(
                "chown -R couchbase:couchbase %s" % data_path)
            BucketUtils.log.info("%s - Done loading bucket %s"
                                 % (shell_conn.ip, b_name))

        node_info = dict()
        for kv_node in kv_nodes:
            shell = RemoteMachineShellConnection(kv_node)
            rest = RestConnection(kv_node)
            n_info = rest.get_nodes_self()
            shell.stop_server()
            node_info[kv_node.ip] = dict()
            node_info[kv_node.ip]["shell"] = shell
            node_info[kv_node.ip]["data_path"] = \
                n_info.storage[0].path

        sleep(5, "Wait for servers to shutdown gracefully")
        if tar_src == "remote":
            # Copy from remote to executor path
            src_ip, src_dir = tar_dir.split(":")
            server_obj = TestInputServer()
            server_obj.ip = src_ip
            server_obj.ssh_username = "root"
            server_obj.ssh_password = "couchbase"
            remote_file_shell = RemoteMachineShellConnection(server_obj)
            remote_file_shell.get_dir(os.path.dirname(src_dir),
                                      os.path.basename(src_dir),
                                      todir=os.path.basename(src_dir))
            remote_file_shell.disconnect()
            # Override remote dir with copied local path
            tar_dir = os.path.basename(src_dir)

        # Copy from executor's path to all kv_nodes' data_path
        bucket_loading_task = list()
        for index, cluster_node in enumerate(kv_nodes):
            bucket_loading_task.append(
                self.task.async_function(
                    load_bucket_on_node,
                    (index + 1,
                     node_info[cluster_node.ip]["shell"],
                     bucket_obj.name,
                     node_info[cluster_node.ip]["data_path"],
                     tar_dir)))

        for task in bucket_loading_task:
            self.task_manager.get_task_result(task)

        for kv_node in kv_nodes:
            node_info[kv_node.ip]["shell"].start_server()
            node_info[kv_node.ip]["shell"].disconnect()
            bucket_obj.servers.append(kv_node)

        # Wait for warmup to complete for the given bucket
        if not self._wait_warmup_completed(bucket_obj):
            self.log.critical("Bucket %s warmup failed after loading from tar"
                              % bucket_obj.name)

    # Support functions with bucket object
    @staticmethod
    def get_bucket_obj(buckets, bucket_name):
        bucket_obj = None
        for bucket in buckets:
            if bucket.name == bucket_name:
                bucket_obj = bucket
                break
        return bucket_obj

    def __print_on_prem_bucket_stats(self, cluster):
        table = TableView(self.log.info)
        buckets = self.get_all_buckets(cluster)
        if len(buckets) == 0:
            table.add_row(["No buckets"])
        else:
            table.set_headers(
                ["Bucket", "Type / Storage", "Replicas", "Rank", "Vbuckets",
                 "Durability", "TTL", "Items",
                 "RAM Quota / Used", "Disk Used", "ARR"])
            if CbServer.cluster_profile == "serverless":
                table.headers += ["Width / Weight"]
            for bucket in buckets:
                num_vbuckets = resident_ratio = storage_backend = "-"
                if bucket.bucketType == Bucket.Type.MEMBASE:
                    storage_backend = bucket.storageBackend
                    try:
                        resident_ratio = BucketHelper(cluster.master).fetch_bucket_stats(
                            bucket.name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
                    except KeyError:
                        resident_ratio = 100
                    num_vbuckets = str(bucket.numVBuckets)
                bucket_data = [
                    bucket.name,
                    "{} / {}".format(bucket.bucketType, storage_backend),
                    str(bucket.replicaNumber), str(bucket.rank), num_vbuckets,
                    str(bucket.durabilityMinLevel),
                    str(bucket.maxTTL), str(bucket.stats.itemCount),
                    "{} / {}".format(humanbytes(str(bucket.stats.ram)),
                                     humanbytes(str(bucket.stats.memUsed))),
                    humanbytes(str(bucket.stats.diskUsed)),
                    resident_ratio]
                if CbServer.cluster_profile == "serverless":
                    if bucket.serverless:
                        bucket_data += ["%s / %s" % (bucket.serverless.width,
                                                     bucket.serverless.weight)]
                    else:
                        bucket_data += ["-"]
                table.add_row(bucket_data)
        table.display("Bucket statistics")

    def __print_serverless_bucket_stats(self, cluster):
        table = TableView(self.log.info)
        if len(cluster.buckets) == 0:
            table.add_row(["No buckets"])
        else:
            table.set_headers(["Bucket", "Width / Weight", "Num Items"])
            for bucket in cluster.buckets:
                bucket_data = [
                    bucket.name,
                    "%s / %s" % (bucket.serverless.width,
                                 bucket.serverless.weight),
                    self.get_expected_total_num_items(bucket)]
                table.add_row(bucket_data)
        table.display("Bucket statistics")

    def print_bucket_stats(self, cluster):
        if cluster.type == "serverless":
            self.__print_serverless_bucket_stats(cluster)
        else:
            self.__print_on_prem_bucket_stats(cluster)

    @staticmethod
    def get_vbucket_num_for_key(doc_key, total_vbuckets=1024):
        """
        Calculates vbucket number based on the document's key

        Argument:
        :doc_key        - Document's key
        :total_vbuckets - Total vbuckets present in the bucket

        Returns:
        :vbucket_number calculated based on the 'doc_key'
        """
        return (((zlib.crc32(doc_key.encode())) >> 16) & 0x7fff) \
               & (total_vbuckets - 1)

    @staticmethod
    def change_max_buckets(cluster_node, total_buckets):
        command = "curl -X POST -u {0}:{1} -d maxBucketCount={2} http://{3}:{4}/internalSettings" \
            .format(cluster_node.rest_username,
                    cluster_node.rest_password, total_buckets,
                    cluster_node.ip, cluster_node.port)
        shell = RemoteMachineShellConnection(cluster_node)
        output, error = shell.execute_command_raw_jsch(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    @staticmethod
    def _set_time_sync_on_buckets(cluster, buckets):
        # get the credentials beforehand
        memcache_credentials = dict()
        for s in cluster.nodes_in_cluster:
            memcache_admin, memcache_admin_password = \
                RestConnection(s).get_admin_credentials()
            memcache_credentials[s.ip] = {'id': memcache_admin,
                                          'password': memcache_admin_password}

        for b in buckets:
            client1 = VBucketAwareMemcached(
                RestConnection(cluster.master), b)

            for j in range(b.vbuckets):
                active_vb = client1.memcached_for_vbucket(j)
                active_vb.sasl_auth_plain(
                    memcache_credentials[active_vb.host]['id'],
                    memcache_credentials[active_vb.host]['password'])
                active_vb.bucket_select(b)
                _ = active_vb.set_time_sync_state(j, 1)

    @staticmethod
    def get_bucket_compressionMode(cluster_node, bucket='default'):
        b_info = BucketHelper(cluster_node).get_buckets_json(bucket_name=bucket)
        return b_info[Bucket.compressionMode]

    def create_multiple_buckets(
            self, cluster, replica,
            bucket_ram_ratio=(2.0 / 3.0),
            bucket_count=3,
            bucket_type=Bucket.Type.MEMBASE,
            eviction_policy=None,
            flush_enabled=Bucket.FlushBucket.DISABLED,
            maxttl=0,
            storage={Bucket.StorageBackend.magma: 3,
                     Bucket.StorageBackend.couchstore: 0},
            compression_mode=Bucket.CompressionMode.ACTIVE,
            bucket_durability=Bucket.DurabilityMinLevel.NONE,
            ram_quota=None,
            bucket_rank=None,
            bucket_name=None,
            purge_interval=1,
            autoCompactionDefined="false",
            fragmentation_percentage=50,
            vbuckets=None,
            weight=None, width=None,
            history_retention_collection_default="true",
            history_retention_bytes=0,
            history_retention_seconds=0,
            warmup_behavior=Bucket.WarmupBehavior.BACKGROUND,
            fusion_log_store_uri=None):
        success = True
        info = self.cluster_util.get_nodes_self(cluster.master)
        tasks = dict()
        if type(storage) is not dict:
            storage = {storage: bucket_count}
        if Bucket.StorageBackend.magma in storage and \
                    storage[Bucket.StorageBackend.magma] > 0:
            if ram_quota is not None and info.memoryQuota < ram_quota * bucket_count:
                self.log.error("At least need {0}MB memoryQuota".format(ram_quota*bucket_count))
                success = False
            elif ram_quota is None and info.memoryQuota < 256 * bucket_count:
                self.log.error("At least need {0}MB memoryQuota".format(256*bucket_count))
                success = False
        if success:
            for key in list(storage.keys()):
                if ram_quota is not None:
                    bucket_ram = ram_quota
                else:
                    available_ram = info.memoryQuota * bucket_ram_ratio
                    bucket_ram = int(available_ram / bucket_count)

                count = 0
                while count < storage[key]:
                    name = "{0}.{1}".format(key, count)
                    if bucket_name is not None:
                        name = "{0}{1}".format(bucket_name, count)
                    bucket = Bucket({
                        Bucket.name: name,
                        Bucket.ramQuotaMB: bucket_ram,
                        Bucket.replicaNumber: replica,
                        Bucket.bucketType: bucket_type,
                        Bucket.evictionPolicy: eviction_policy,
                        Bucket.flushEnabled: flush_enabled,
                        Bucket.maxTTL: maxttl,
                        Bucket.rank: bucket_rank,
                        Bucket.storageBackend: key,
                        Bucket.compressionMode: compression_mode,
                        Bucket.durabilityMinLevel: bucket_durability,
                        Bucket.purge_interval: purge_interval,
                        Bucket.autoCompactionDefined: autoCompactionDefined,
                        Bucket.fragmentationPercentage: fragmentation_percentage,
                        Bucket.numVBuckets: vbuckets,
                        Bucket.weight: weight,
                        Bucket.width: width,
                        Bucket.historyRetentionCollectionDefault: history_retention_collection_default,
                        Bucket.historyRetentionSeconds: history_retention_seconds,
                        Bucket.historyRetentionBytes: history_retention_bytes,
                        Bucket.warmupBehavior: warmup_behavior,
                        Bucket.fusionLogstoreURI: fusion_log_store_uri})
                    tasks[bucket] = self.async_create_bucket(cluster, bucket)
                    count += 1

            raise_exception = None
            for bucket, task in tasks.items():
                self.task_manager.get_task_result(task)
                if task.result:
                    cluster.buckets.append(bucket)
                else:
                    raise_exception = "Create bucket %s failed" % bucket.name

            # Check for warm_up
            for bucket in cluster.buckets:
                self.get_updated_bucket_server_list(cluster, bucket)
                warmed_up = self._wait_warmup_completed(bucket, wait_time=300)
                if not warmed_up:
                    success = False
                    raise_exception = "Bucket %s not warmed up" % bucket.name

            if raise_exception:
                raise Exception(raise_exception)
        return success

    def flush_bucket(self, cluster, bucket, skip_resetting_num_items=False):
        self.log.debug('Flushing existing bucket {0} on {1}'
                       .format(bucket, cluster.master))
        bucket_conn = BucketHelper(cluster.master)
        if self.bucket_exists(cluster, bucket):
            status = bucket_conn.flush_bucket(bucket.name)
            if not status:
                self.log.error("Flush bucket '{0}' failed from {1}"
                               .format(bucket, cluster.master.ip))
            else:
                # Mark all existing collections as flushed
                for s_name in BucketUtils.get_active_scopes(bucket,
                                                            only_names=True):
                    for c_name in BucketUtils.get_active_collections(
                            bucket, s_name, only_names=True):
                        BucketUtils.mark_collection_as_flushed(
                            bucket, s_name, c_name,
                            skip_resetting_num_items=skip_resetting_num_items)
            return status

    def flush_all_buckets(self, cluster, skip_resetting_num_items=False):
        status = dict()
        self.log.debug("Flushing existing buckets '%s'"
                       % [bucket.name for bucket in cluster.buckets])
        for bucket in cluster.buckets:
            status[bucket] = self.flush_bucket(
                cluster, bucket,
                skip_resetting_num_items=skip_resetting_num_items)
        return status

    @staticmethod
    def update_bucket_property(cluster_node, bucket, ram_quota_mb=None,
                               replica_number=None, replica_index=None,
                               flush_enabled=None, time_synchronization=None,
                               max_ttl=None, compression_mode=None,
                               eviction_policy=None,
                               storageBackend=None, bucket_rank=None,
                               bucket_durability=None, bucket_width=None,
                               bucket_weight=None,
                               history_retention_collection_default=None,
                               history_retention_bytes=None,
                               history_retention_seconds=None,
                               magma_key_tree_data_block_size=None,
                               magma_seq_tree_data_block_size=None,
                               durability_impossible_fallback=None,
                               warmup_behavior=None):
        return BucketHelper(cluster_node).change_bucket_props(
            bucket, ramQuotaMB=ram_quota_mb, replicaNumber=replica_number,
            replicaIndex=replica_index, flushEnabled=flush_enabled,
            timeSynchronization=time_synchronization, maxTTL=max_ttl,
            compressionMode=compression_mode,
            eviction_policy=eviction_policy,
            bucket_durability=bucket_durability, bucketWidth=bucket_width,
            bucketWeight=bucket_weight,
            bucket_rank=bucket_rank,
            history_retention_collection_default=history_retention_collection_default,
            history_retention_seconds=history_retention_seconds,
            history_retention_bytes=history_retention_bytes,
            magma_key_tree_data_block_size=magma_key_tree_data_block_size,
            magma_seq_tree_data_block_size=magma_seq_tree_data_block_size,
            storageBackend=storageBackend,
            durability_impossible_fallback=durability_impossible_fallback,
            warmup_behavior=warmup_behavior)

    def update_all_bucket_maxTTL(self, cluster, maxttl=0):
        for bucket in cluster.buckets:
            self.log.debug("Updating maxTTL for bucket %s to %ss"
                           % (bucket.name, maxttl))
            self.update_bucket_property(cluster.master, bucket,
                                        max_ttl=maxttl)

    def update_all_bucket_replicas(self, cluster, replicas=1):
        for bucket in cluster.buckets:
            self.log.debug("Updating replica for bucket %s to %ss"
                           % (bucket.name, replicas))
            self.update_bucket_property(cluster.master, bucket,
                                        replica_number=replicas)

    def is_warmup_complete(self, buckets, retry_count=5):
        while retry_count != 0:
            buckets_warmed_up = True
            for bucket in buckets:
                try:
                    warmed_up = self._wait_warmup_completed(bucket,
                                                            wait_time=300)
                    if not warmed_up:
                        buckets_warmed_up = False
                        break
                except:
                    buckets_warmed_up = False
                    break

            if buckets_warmed_up:
                return True

            retry_count -= 1
        return False

    def verify_cluster_stats(self, cluster, items,
                             timeout=None, check_items=True,
                             check_bucket_stats=True,
                             check_ep_items_remaining=False,
                             verify_total_items=True, num_zone=1):

        self._wait_for_stats_all_buckets(
            cluster, cluster.buckets,
            timeout=(timeout or 120),
            check_ep_items_remaining=check_ep_items_remaining)
        if check_items:
            if check_bucket_stats:
                self.verify_stats_all_buckets(cluster, items=items,
                                              timeout=(timeout or 120),
                                              num_zone=num_zone)
            if verify_total_items:
                verified = True
                for bucket in cluster.buckets:
                    verified &= self.wait_till_total_numbers_match(
                        cluster, bucket, timeout_in_seconds=(timeout or 500),
                        num_zone=num_zone)
                if not verified:
                    msg = "Lost items!!! Replication was completed " \
                          "but sum (curr_items) don't match the " \
                          "curr_items_total"
                    self.log.error(msg)
                    raise Exception(msg)

    def get_min_nodes_in_zone(self, cluster):
        rest = RestConnection(cluster.master)
        min_count = list()
        zones = rest.get_zone_names()
        for zone in zones:
            nodes = list(rest.get_nodes_in_zone(zone).keys())
            if nodes:
                min_count.append(len(nodes))
        # [2,2,1] supports replica 3
        # But [1,7,1] (even though zone 2 has more nodes) supports only replica 2
        # In general, addition of nodes in two zones >=3 to support 3 replicas
        if len(min_count) == 3:
            if min_count.count(1) >= 2:
                return 2
            return 3
        return min(min_count)

    def verify_stats_for_bucket(self, cluster, bucket, items, timeout=60, num_zone=1):
        self.log.debug("Verifying stats for bucket {0}".format(bucket.name))
        stats_tasks = []
        servers = self.cluster_util.get_bucket_kv_nodes(cluster, bucket)
        # TODO: Need to fix the config files to always satisfy the
        #       replica number based on the available number_of_servers
        available_replicas = bucket.replicaNumber
        if num_zone > 1:
            """
            replica number will be either replica number or one less than the zone number
            or min number of nodes in the zone
            zone =2:
                1 node each, replica set =1, actual =1
                1 node each, replica set >1, actual = 1
                2 nodes each, replica set =2, actual =2
                2 nodes each, replica set =3, actual =2
                3 nodes each, replica set =3, actual =3
                group1: 2 nodes, group 1: 1 node, replica_set >1, actual = 1
                group1: 3 nodes, group 1: 2 node, replica_set >=2, actual = 2
                group1: 4 nodes, group 1: 5 node, replica_set =3, actual = 3

            zone =3:
                1 node each, replica_set=2, actual =2
                2 nodes each, relica_set =3, actual =3
                3 nodes each,repica_set = 3, actaul = 3
                num_node_in_each_group: [1, 2, 1], replica_set=3, actual = 2
                num_node_in_each_group: [2, 2, 1], replica_set=3, actual = 3
                num_node_in_each_group: [3, 2, 1], replica_set=3, actual = 3
                num_node_in_each_group: [3, 3, 1], replica_set=3, actual = 3
            """
            if bucket.replicaNumber >= num_zone:
                available_replicas = self.get_min_nodes_in_zone(cluster)
                if available_replicas > bucket.replicaNumber:
                    available_replicas = bucket.replicaNumber
            self.log.info("available_replicas considered {0}".format(available_replicas))
        else:
            if len(servers) == bucket.replicaNumber:
                available_replicas = len(servers) - 1
            elif len(servers) <= bucket.replicaNumber:
                available_replicas = len(servers) - 1

        # Create connection to master node for verifying cbstats
        stat_cmd = "all"

        # Create Tasks to verify total items/replica count in the bucket
        stats_tasks.append(self.task.async_wait_for_stats(
            servers, bucket, stat_cmd,
            'vb_replica_curr_items', '==', items * available_replicas,
            timeout=timeout))
        stats_tasks.append(self.task.async_wait_for_stats(
            servers, bucket, stat_cmd,
            'curr_items_tot', '==', items * (available_replicas + 1),
            timeout=timeout))
        try:
            for task in stats_tasks:
                self.task_manager.get_task_result(task)
        except Exception as e:
            self.log.error("{0}".format(e))
            for task in stats_tasks:
                self.task_manager.stop_task(task)
            self.log.error("Unable to get expected stats from the "
                           "selected node")

    def verify_stats_all_buckets(self, cluster, items, timeout=1200,
                                 num_zone=1, buckets=None):
        buckets = buckets or cluster.buckets
        for bucket in buckets:
            vbucket_stats = self.get_vbucket_seqnos(
                self.cluster_util.get_bucket_kv_nodes(cluster, bucket),
                [bucket],
                skip_consistency=True)
            self.verify_stats_for_bucket(cluster, bucket, items,
                                         timeout=timeout, num_zone=num_zone)
            #Validate seq_no snap_start/stop values with initial load
            result = self.validate_seq_no_stats(vbucket_stats[bucket.name])
            self.assertTrue(result,
                            "snap_start and snap_end corruption found!!!")

    def _wait_for_stat(self, bucket, stat_map=None,
                       cbstat_cmd="checkpoint",
                       stat_name="persistence:num_items_for_cursor",
                       stat_cond='==',
                       timeout=60):
        """
        Waits for queues to drain on all servers and buckets in a cluster.

        A utility function that waits for all of the items loaded to be
        persisted and replicated.

        Arguments:
        :param bucket: Bucket object used for verification
        :param stat_map: Dict of node_ip:expected stat value
        :param cbstat_cmd: cbstat command name to execute
        :param stat_name: Stat to validate against the expected value
        :param stat_cond: Binary condition used for validation
        :param timeout: Waiting the end of the thread. (str)
        """
        tasks = []
        if stat_map:
            for server, stat_value in stat_map.items():
                if bucket.bucketType == 'memcached':
                    continue
                tasks.append(self.task.async_wait_for_stats(
                    [server], bucket, cbstat_cmd,
                    stat_name, stat_cond, stat_value,
                    timeout=timeout))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def _wait_for_stats_all_buckets(self, cluster, buckets, expected_val=0,
                                    comparison_condition='==',
                                    check_ep_items_remaining=False,
                                    timeout=600,
                                    cbstat_cmd="checkpoint",
                                    stat_name="persistence:num_items_for_cursor"):
        """
        Waits for queues to drain on all servers and buckets in a cluster.

        A utility function that waits for all of the items loaded to be
        persisted and replicated.

        Args:
          servers - List of all servers in the cluster ([TestInputServer])
          ep_queue_size - expected ep_queue_size (int)
          ep_queue_size_cond - condition for comparing (str)
          check_ep_dcp_items_remaining - to check if replication is complete
          timeout - Waiting the end of the thread. (str)
        """
        tasks = list()
        for bucket in buckets:
            if cluster.type != "serverless":
                self.get_updated_bucket_server_list(cluster, bucket)
            if bucket.bucketType == 'memcached':
                continue
            for server in bucket.servers:
                if check_ep_items_remaining:
                    dcp_cmd = "dcp"
                    tasks.append(self.task.async_wait_for_stats(
                        [server], bucket, dcp_cmd,
                        'ep_dcp_items_remaining', "==", 0,
                        timeout=timeout))
                tasks.append(self.task.async_wait_for_stats(
                    [server], bucket, cbstat_cmd,
                    stat_name, comparison_condition, expected_val,
                    timeout=timeout))
                time.sleep(1)
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def validate_active_replica_item_count(self, cluster, bucket, timeout=300):
        end_time = time.time() + timeout
        active_count = None
        replica_count = None
        while time.time() < end_time:
            bucket_helper = BucketHelper(cluster.master)
            bucket_stats = bucket_helper.fetch_bucket_stats(bucket.name)
            try:
                active_count = bucket_stats['op']['samples']['curr_items'][-1]
                replica_count = bucket_stats['op']['samples'][
                    'vb_replica_curr_items'][-1]
                self.log.debug("Bucket '%s' items count. Active: %s, Replica : %s"
                       % (bucket.name, active_count, replica_count))
            except KeyError:
                continue
            if (active_count * bucket.replicaNumber) == replica_count:
                return True
        self.log.critical("Mismatch!!! Bucket '%s' items count. Active: %s, Replica : %s"
                           % (bucket.name, active_count, replica_count))
        return False

    def validate_seq_no_stats(self, vb_seqno_stats):
        """
        :param vb_seqno_stats: stat_dictionary returned from
                               Cbstats.vbucket_seqno
        :return validation_passed: Boolean saying validation passed or not
        """
        validation_passed = True
        error_msg = "Node: %s, VB %s - %s > %s: %s > %s"
        comparion_key_pairs = [
            ("high_completed_seqno", "high_seqno"),
            ("last_persisted_snap_start", "last_persisted_seqno"),
            ("last_persisted_snap_start", "last_persisted_snap_end")
        ]
        for node, vBucket in vb_seqno_stats.items():
            for vb_num, stat in vBucket.items():
                for key_pair in comparion_key_pairs:
                    if int(stat[key_pair[0]]) > int(stat[key_pair[1]]):
                        validation_passed = False
                        self.log.error(error_msg % (node,
                                                    vb_num,
                                                    key_pair[0],
                                                    key_pair[1],
                                                    stat[key_pair[0]],
                                                    stat[key_pair[1]]))

        return validation_passed

    def validate_history_retention_settings(self, kv_node, buckets):
        result = True
        cb_stat = Cbstats(kv_node)
        if not isinstance(buckets, list):
            buckets = [buckets]
        self.log.debug("Validating history retention settings on node {0}"
                       .format(kv_node.ip))
        for bucket in buckets:
            if bucket.bucketType == Bucket.Type.EPHEMERAL or \
                bucket.storageBackend == Bucket.StorageBackend.couchstore:
                continue
            stat = cb_stat.all_stats(bucket.name)
            if bucket.storageBackend != Bucket.StorageBackend.magma:
                for s_field in ["ep_history_retention_bytes",
                                "ep_history_retention_seconds"]:
                    if s_field in stat:
                        result = False
                        self.log.critical("Field {0} present in all_stats"
                                          .format(s_field))
            else:
                self.log.debug("Hist. retention bytes={0}, seconds={1}"
                               .format(stat["ep_history_retention_bytes"],
                                  stat["ep_history_retention_seconds"]))
                if int(stat["ep_history_retention_bytes"]) \
                        != int(bucket.historyRetentionBytes):
                    result = False
                    self.log.critical("Hist retention bytes mismatch. "
                                      "Expected: {0}, Actual: {1}"
                                      .format(bucket.historyRetentionBytes,
                                         stat["ep_history_retention_bytes"]))
                if int(stat["ep_history_retention_seconds"]) \
                        != int(bucket.historyRetentionSeconds):
                    result = False
                    self.log.critical("Hist retention seconds mismatch. "
                                      "Expected: {0}, Actual: {1}"
                                      .format(bucket.historyRetentionSeconds,
                                         stat["ep_history_retention_seconds"]))

            stat = cb_stat.get_collections(bucket)
            for s_name, scope in bucket.scopes.items():
                if scope.is_dropped:
                    continue
                for c_name, col in scope.collections.items():
                    if col.is_dropped:
                        continue
                    col = bucket.scopes[s_name].collections[c_name]
                    val_as_per_test = col.history
                    val_as_per_stat = stat[s_name][c_name]["history"]
                    log_msg = "%s - %s:%s:%s - Expected %s. Actual: %s" \
                              % (kv_node.ip, bucket.name, s_name, c_name,
                                 val_as_per_test, val_as_per_stat)
                    self.log.debug(log_msg)
                    if val_as_per_test != val_as_per_stat:
                        result = False
                        self.log.critical(log_msg)
        cb_stat.disconnect()
        return result

    def validate_oso_dcp_backfill_value(self, kv_nodes, buckets,
                                        expected_val="auto"):
        result = True
        if expected_val is None:
            expected_val = "auto"
        elif expected_val == 1:
            expected_val = 'true'
        elif expected_val == 0:
            expected_val = 'false'

        for node in kv_nodes:
            cbstat = Cbstats(node)
            for bucket in buckets:
                val = cbstat.all_stats(bucket.name)["ep_dcp_oso_backfill"]
                if val != expected_val:
                    result = False
                    self.log.critical("Bucket {}, oso_dcp_backfill mismatch."
                                      "Expected {}, Actual: {}"
                                      .format(bucket.name, expected_val, val))
            cbstat.disconnect()
        return result

    def wait_for_collection_creation_to_complete(self, cluster, timeout=60):
        self.log.info("Waiting for all collections to be created")
        bucket_helper = BucketHelper(cluster.master)
        for bucket in cluster.buckets:
            start_time = time.time()
            stop_time = start_time + timeout
            count_matched = False
            total_collection_as_per_bucket_obj = \
                self.get_total_collections_in_bucket(bucket)
            while time.time() < stop_time:
                total_collection_as_per_stats = \
                    bucket_helper.get_total_collections_in_bucket(bucket)
                if total_collection_as_per_bucket_obj \
                        == total_collection_as_per_stats:
                    count_matched = True
                    break
            if not count_matched:
                self.log.error("Collections count mismatch in %s. %s != %s"
                               % (bucket.name,
                                  total_collection_as_per_bucket_obj,
                                  total_collection_as_per_stats))
                raise Exception("Collection count mismatch for bucket: %s. "
                                "Expected: %d, Actual: %d"
                                % (bucket.name,
                                   total_collection_as_per_bucket_obj,
                                   total_collection_as_per_stats))

    @staticmethod
    def get_doc_op_info_dict(bucket, op_type, exp=0, replicate_to=0,
                             persist_to=0, durability="",
                             timeout=5, time_unit="seconds",
                             scope=CbServer.default_scope,
                             collection=CbServer.default_collection,
                             ignore_exceptions=[], retry_exceptions=[]):
        info_dict = dict()
        info_dict["ops_failed"] = False
        info_dict["bucket"] = bucket
        info_dict["scope"] = scope
        info_dict["collection"] = collection
        info_dict["op_type"] = op_type
        info_dict["exp"] = exp
        info_dict["replicate_to"] = replicate_to
        info_dict["persist_to"] = persist_to
        info_dict["durability"] = durability
        info_dict["timeout"] = timeout
        info_dict["time_unit"] = time_unit
        info_dict["ignore_exceptions"] = ignore_exceptions
        info_dict["retry_exceptions"] = retry_exceptions
        info_dict["retried"] = {"success": dict(), "fail": dict()}
        info_dict["unwanted"] = {"success": dict(), "fail": dict()}
        info_dict["ignored"] = dict()

        return info_dict

    @staticmethod
    def doc_ops_tasks_status(tasks_info):
        """
        :param tasks_info: dict with "ops_failed" Bool value updated
        :return: Aggregated success status for all tasks. (Boolean)
        """
        for task, _ in tasks_info.items():
            if tasks_info[task]["ops_failed"]:
                return False
        return True

    def log_doc_ops_task_failures(self, tasks_info):
        """
        Validated all exceptions and retires the doc_ops if required
        from each task within the tasks_info dict().

        If doc failures are seen, task["ops_failed"] will be marked as True

        :param tasks_info: dictionary updated with retried/unwanted docs
        """
        for task, task_info in tasks_info.items():
            task.result = True
            op_type = task_info["op_type"]
            ignored_keys = list(task_info["ignored"].keys())
            retried_success_keys = list(task_info["retried"]["success"].keys())
            retried_failed_keys = list(task_info["retried"]["fail"].keys())
            unwanted_success_keys = \
                list(task_info["unwanted"]["success"].keys())
            unwanted_failed_keys = list(task_info["unwanted"]["fail"].keys())

            # Success cases
            if len(ignored_keys) > 0:
                self.log.debug("Ignored exceptions for '{0}': ({1}): {2}"
                               .format(op_type, len(ignored_keys),
                                       ignored_keys))

            if len(retried_success_keys) > 0:
                self.log.debug("Docs succeeded for expected retries "
                               "for '{0}' ({1}): {2}"
                               .format(op_type, len(retried_success_keys),
                                       retried_success_keys))

            # Failure cases
            if len(retried_failed_keys) > 0:
                task_info["ops_failed"] = True
                task.result = False
                self.log.error("Docs failed after expected retry "
                               "for '{0}' ({1}): {2}"
                               .format(op_type, len(retried_failed_keys),
                                       retried_failed_keys))
                self.log.error("Exceptions for failure on retried docs: {0}"
                               .format(task_info["retried"]["fail"]))

            if len(unwanted_success_keys) > 0:
                task_info["ops_failed"] = True
                task.result = False
                self.log.error("Unexpected exceptions, succeeded "
                               "after retry for '{0}' ({1}): {2}"
                               .format(op_type, len(unwanted_success_keys),
                                       unwanted_success_keys))

            if len(unwanted_failed_keys) > 0:
                task_info["ops_failed"] = True
                task.result = False
                self.log.error("Unexpected exceptions, failed even "
                               "after retry for '{0}' ({1}): {2}"
                               .format(op_type, len(unwanted_failed_keys),
                                       unwanted_failed_keys))
                self.log.error("Exceptions for unwanted doc failures "
                               "after retry: {0}"
                               .format(task_info["unwanted"]["fail"]))

    def verify_doc_op_task_exceptions_with_sirius(self, tasks_info, cluster):
        WorkLoadTask = sirius_task.WorkLoadTask
        for task, task_info in list(tasks_info.items()):
            bucket = task_info["bucket"]
            scope = task_info["scope"]
            collection = task_info["collection"]
            task =  sirius_task.WorkLoadTask(self.task_manager,
                                             task_info["op_type"],
                                             )
            sirius_task.LoadCouchbaseDocs(
                self.task_manager, cluster, CbServer.use_https,
                bucket, scope, collection,)
            exception_payload = client.create_payload_exception_handling(
                resultSeed=task.resultSeed,
                identifierToken=IDENTIFIER_TOKEN,
                ignoreExceptions=[], retryExceptions=[],
                retryAttempts=1)
            failed_docs, _, _, _ = client.retry_exceptions(
                exception_payload=exception_payload, delete_record=False)
            for key, failed_doc in list(failed_docs.items()):
                found = False
                exception = failed_doc["error"]
                key_value = {key: failed_doc}
                for ex in task_info["ignore_exceptions"]:
                    if DocLoaderUtils.check_if_exception_exists(exception, ex):
                        bucket.scopes[scope].collections[
                            collection].num_items -= 1
                        tasks_info[task]["ignored"].update(key_value)
                        found = True
                        break
                if found:
                    continue
                dict_key = "unwanted"
                for ex in task_info["retry_exceptions"]:
                    if DocLoaderUtils.check_if_exception_exists(str(exception), ex):
                        dict_key = "retried"
                        break
                if failed_doc["status"]:
                    tasks_info[task][dict_key]["success"].update(key_value)
                else:
                    tasks_info[task][dict_key]["fail"].update(key_value)
        return tasks_info

    def verify_doc_op_task_exceptions(self, tasks_info, cluster,
                                      load_using="default_loader"):
        """
        :param tasks_info:  dict() of dict() of form,
                            tasks_info[task_obj] = get_doc_op_info_dict()
        :param cluster:     Cluster object
        :param load_using: Param to tell to use inbuilt / sirius SDK
        :return: tasks_info dictionary updated with retried/unwanted docs
        """

        if load_using == "sirius_go_sdk":
            # Sirius Golang case
            return self.verify_doc_op_task_exceptions_with_sirius(
                tasks_info, cluster)

        # Default / Java (via sirius) SDK case
        for task, task_info in list(tasks_info.items()):
            client = None
            bucket = task_info["bucket"]
            scope = task_info["scope"]
            collection = task_info["collection"]

            if cluster.sdk_client_pool:
                client = cluster.sdk_client_pool.get_client_for_bucket(
                    bucket, scope, collection)

            if client is None:
                client = SDKClient(cluster, bucket,
                                   scope=scope, collection=collection)

            for key, failed_doc in task.fail.items():
                found = False
                exception = failed_doc["error"]
                key_value = {key: failed_doc}
                for ex in task_info["ignore_exceptions"]:
                    if SDKException.check_if_exception_exists(ex, exception):
                        bucket \
                            .scopes[scope] \
                            .collections[collection].num_items -= 1
                        tasks_info[task]["ignored"].update(key_value)
                        found = True
                        break
                if found:
                    continue

                ambiguous_state = False
                if SDKException.check_if_exception_exists(
                        SDKException.DurabilityAmbiguousException
                        + SDKException.AmbiguousTimeoutException
                        + SDKException.TimeoutException
                        + SDKException.RequestCanceledException, exception):
                    ambiguous_state = True

                val = None
                # If DELETE, val can stay None since it has no impact
                if task_info["op_type"] != DocLoading.Bucket.DocOps.DELETE:
                    if "value" in failed_doc:
                        # In Java loader case, the task returns the value
                        val = failed_doc["value"]
                    else:
                        # Python: the value is not preserved, so we regenerate
                        doc_gen = task.generators[0]
                        doc_gen.itr = int(key.split("-")[-1])
                        _, val = doc_gen.next()
                result = client.crud(
                    task_info["op_type"], key, val,
                    exp=task_info["exp"],
                    replicate_to=task_info["replicate_to"],
                    persist_to=task_info["persist_to"],
                    durability=task_info["durability"],
                    timeout=task_info["timeout"],
                    time_unit=task_info["time_unit"])

                dict_key = "unwanted"
                for ex in task_info["retry_exceptions"]:
                    if SDKException.check_if_exception_exists(ex, exception):
                        dict_key = "retried"
                        break
                if result["status"] \
                        or (ambiguous_state
                            and SDKException.check_if_exception_exists(
                                SDKException.DocumentExistsException,
                                result["error"])
                            and task_info["op_type"] in ["create", "update"]) \
                        or (ambiguous_state
                            and SDKException.check_if_exception_exists(
                                SDKException.DocumentNotFoundException,
                                result["error"])
                            and task_info["op_type"] == "delete"):
                    tasks_info[task][dict_key]["success"].update(key_value)
                else:
                    tasks_info[task][dict_key]["fail"].update(key_value)
            if cluster.sdk_client_pool:
                cluster.sdk_client_pool.release_client(client)
            else:
                # Close client for this task
                client.close()

        return tasks_info

    def async_load_bucket(self, cluster, bucket, generator, op_type,
                          exp=0, random_exp=False,
                          flag=0, persist_to=0, replicate_to=0,
                          durability="", sdk_timeout=5, time_unit="seconds",
                          batch_size=10,
                          compression=True, process_concurrency=8, retries=5,
                          active_resident_threshold=100,
                          ryow=False, check_persistence=False,
                          skip_read_on_error=False, suppress_error_table=False,
                          dgm_batch=5000,
                          scope=CbServer.default_scope,
                          collection=CbServer.default_collection,
                          monitor_stats=["doc_ops"],
                          track_failures=True,
                          sdk_retry_strategy=None,
                          iterations=1, load_using="default_loader"):
        return self.task.async_load_gen_docs(
            cluster, bucket, generator, op_type,
            exp=exp, random_exp=random_exp,
            flag=flag, persist_to=persist_to, replicate_to=replicate_to,
            durability=durability, timeout_secs=sdk_timeout, time_unit=time_unit,
            batch_size=batch_size,
            compression=compression,
            process_concurrency=process_concurrency, retries=retries,
            active_resident_threshold=active_resident_threshold,
            ryow=ryow, check_persistence=check_persistence,
            suppress_error_table=suppress_error_table,
            skip_read_on_error=skip_read_on_error,
            dgm_batch=dgm_batch,
            scope=scope, collection=collection,
            monitor_stats=monitor_stats,
            track_failures=track_failures,
            sdk_retry_strategy=sdk_retry_strategy,
            iterations=iterations, load_using=load_using)

    def load_docs_to_all_collections(self, start, end, cluster,
                                     key="test_docs",
                                     op_type=DocLoading.Bucket.DocOps.CREATE,
                                     mutate=0, process_concurrency=8,
                                     persist_to=0, replicate_to=0,
                                     batch_size=10,
                                     timeout_secs=10,
                                     load_using="default_loader"):
        """
        Loads documents to all the collections available in bucket_util sequentially
        :param start int, starting of document serial
        :param end int, ending of document serial
        :param: key string, prefix for the document name
        :param: cluster CBCluster object, cluster on which loading has to be done
        :param: op_type DocLoading.Bucket.DocOps type, type of operation
        correctness of num_items update in bucket object
        if loading asynchronously then it is suggested to wait for the results of
        all the tasks and validate doc_count explicitly
        """
        generator = doc_generator(key, start, end, mutate=mutate)
        for bucket in self.get_all_buckets(cluster):
            for _, scope in bucket.scopes.items():
                if scope.name == "_system":
                    continue
                for _, collection in scope.collections.items():
                    task = self.task.async_load_gen_docs(
                        cluster, bucket,
                        generator, op_type,
                        batch_size=batch_size,
                        scope=scope.name,
                        collection=collection.name,
                        process_concurrency=process_concurrency,
                        persist_to=persist_to, replicate_to=replicate_to,
                        timeout_secs=timeout_secs,
                        load_using=load_using)
                    self.task_manager.get_task_result(task)
                    bucket.scopes[scope.name].collections[collection.name].num_items += (end - start)
        # Doc count validation
        self._wait_for_stats_all_buckets(cluster, cluster.buckets)
        self.validate_docs_per_collections_all_buckets(cluster)

    def _async_load_all_buckets(self, cluster, kv_gen, op_type,
                                exp, random_exp=False,
                                flag=0, persist_to=0, replicate_to=0,
                                batch_size=1,
                                timeout_secs=30, time_unit="seconds",
                                sdk_compression=True, process_concurrency=8,
                                retries=5, durability="",
                                ignore_exceptions=[], retry_exceptions=[],
                                active_resident_threshold=100,
                                ryow=False, check_persistence=False,
                                skip_read_on_error=False, suppress_error_table=False,
                                dgm_batch=5000,
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection,
                                monitor_stats=["doc_ops"],
                                track_failures=True,
                                sdk_retry_strategy=None,
                                iterations=1, load_using="default_loader"):

        """
        Asynchronously apply load generation to all buckets in the
        cluster.bucket.name, gen, op_type, exp
        Args:
            server - A server in the cluster. (TestInputServer)
            kv_gen - The generator to use to generate load. (DocumentGenerator)
            op_type - "create", "read", "update", or "delete" (String)
            exp - The expiration for the items if updated or created (int)

        Returns:
            task_info - dict of dict populated using get_doc_op_info_dict()
        """
        tasks_info = dict()
        for bucket in cluster.buckets:
            task = self.async_load_bucket(
                cluster, bucket, kv_gen, op_type, exp, random_exp,
                flag, persist_to,
                replicate_to, durability, timeout_secs, time_unit,
                batch_size,
                sdk_compression, process_concurrency, retries,
                active_resident_threshold=active_resident_threshold,
                ryow=ryow, check_persistence=check_persistence,
                suppress_error_table=suppress_error_table,
                skip_read_on_error=skip_read_on_error,
                dgm_batch=dgm_batch,
                scope=scope, collection=collection,
                monitor_stats=monitor_stats,
                track_failures=track_failures,
                sdk_retry_strategy=sdk_retry_strategy,
                iterations=iterations, load_using=load_using)
            tasks_info[task] = self.get_doc_op_info_dict(
                bucket, op_type, exp,
                scope=scope,
                collection=collection,
                replicate_to=replicate_to,
                persist_to=persist_to,
                durability=durability,
                timeout=timeout_secs, time_unit=time_unit,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
        return tasks_info

    def _async_validate_docs(self, cluster, kv_gen, op_type, exp,
                             flag=0, batch_size=1,
                             timeout_secs=5, time_unit="seconds",
                             compression=True,
                             process_concurrency=4, check_replica=False,
                             ignore_exceptions=[], retry_exceptions=[],
                             scope=CbServer.default_scope,
                             collection=CbServer.default_collection,
                             suppress_error_table=False,
                             sdk_retry_strategy=None,
                             validate_using="default_loader"):
        task_info = dict()
        for bucket in cluster.buckets:
            gen = copy.deepcopy(kv_gen)
            task = self.task.async_validate_docs(
                cluster, bucket, gen, op_type, exp, flag,
                batch_size, timeout_secs, time_unit, compression,
                process_concurrency, check_replica,
                scope, collection,
                suppress_error_table=suppress_error_table,
                sdk_retry_strategy=sdk_retry_strategy,
                validate_using=validate_using)
            task_info[task] = self.get_doc_op_info_dict(
                bucket, op_type, exp,
                scope=scope,
                collection=collection,
                timeout=timeout_secs, time_unit=time_unit,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            return task_info

    def sync_load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                              persist_to=0, replicate_to=0,
                              batch_size=1,
                              timeout_secs=30,
                              sdk_compression=True, process_concurrency=8,
                              retries=5, durability="",
                              ignore_exceptions=list(),
                              retry_exceptions=list(),
                              active_resident_threshold=100,
                              ryow=False, check_persistence=False,
                              skip_read_on_error=False,
                              suppress_error_table=False,
                              dgm_batch=5000,
                              scope=CbServer.default_scope,
                              collection=CbServer.default_collection,
                              monitor_stats=["doc_ops"],
                              track_failures=True,
                              load_using="default_loader"):

        """
        Asynchronously apply load generation to all buckets in the
        cluster.bucket.name, gen, op_type, exp
        Then wait for all doc_loading tasks to complete and verify the
        task's results and retry failed_docs if required.
        Args:
            server - A server in the cluster. (TestInputServer)
            kv_gen - The generator to use to generate load. (DocumentGenerator)
            op_type - "create", "read", "update", or "delete" (String)
            exp - The expiration for the items if updated or created (int)

        Returns:
            task_info - dict of dict populated using get_doc_op_info_dict()
        """
        # Start doc_loading in all buckets in async manner
        tasks_info = self._async_load_all_buckets(
            cluster, kv_gen, op_type, exp=exp,
            sdk_compression=sdk_compression,
            persist_to=persist_to, replicate_to=replicate_to,
            durability=durability,
            batch_size=batch_size, timeout_secs=timeout_secs,
            process_concurrency=process_concurrency,
            ignore_exceptions=ignore_exceptions,
            retry_exceptions=retry_exceptions, ryow=ryow,
            active_resident_threshold=active_resident_threshold,
            check_persistence=check_persistence,
            skip_read_on_error=skip_read_on_error,
            suppress_error_table=suppress_error_table,
            dgm_batch=dgm_batch,
            scope=scope, collection=collection,
            monitor_stats=monitor_stats,
            track_failures=track_failures, load_using=load_using)

        for task in list(tasks_info.keys()):
            self.task_manager.get_task_result(task)

        # Wait for all doc_loading tasks to complete and populate failures
        self.verify_doc_op_task_exceptions(tasks_info, cluster,
                                           load_using=load_using)
        self.log_doc_ops_task_failures(tasks_info)
        return tasks_info

    def load_durable_aborts(self, ssh_shell, server, load_gens, cluster, bucket,
                            durability_level, doc_op="create",
                            load_type="all_aborts",
                            load_using="default_loader"):
        """
        :param ssh_shell: ssh connection for simulating memcached_stop
        :param load_gens: doc_load generators used for running doc_ops
        :param cluster: Cluster object to be utilized by doc_load_task
        :param bucket: Bucket object for loading data
        :param durability_level: Durability level to use while loading
        :param doc_op: Type of doc_operation to perform. create/update/delete
        :param load_type: Data loading pattern.
                          Supports: all_aborts, initial_aborts, aborts_at_end,
                                    mixed_aborts
                          Default is 'all_aborts'
        :return result: 'True' if got expected result during doc_loading.
                        'False' if received unexpected result.
        """

        def get_num_items(doc_gen):
            if doc_gen.pre_generated_keys:
                return len(doc_gen.pre_generated_keys)
            return doc_gen.end

        def load_docs(doc_gen, load_for=""):
            if load_for == "abort":
                return self.task.async_load_gen_docs(
                    cluster, bucket, doc_gen, doc_op, 0,
                    batch_size=10, process_concurrency=8,
                    durability=durability_level,
                    timeout_secs=2, start_task=False,
                    skip_read_on_error=True, suppress_error_table=True,
                    load_using=load_using)
            return self.task.async_load_gen_docs(
                cluster, bucket, doc_gen, doc_op, 0,
                batch_size=10, process_concurrency=8,
                durability=durability_level,
                timeout_secs=60, load_using=load_using)

        result = True
        tasks = list()
        num_items = dict()
        cb_err = CouchbaseError(self.log,
                                ssh_shell,
                                node=server)

        if load_type == "initial_aborts":
            # Initial abort task
            task = load_docs(load_gens[0], "abort")
            num_items[task] = get_num_items(load_gens[0])
            cb_err.create(CouchbaseError.STOP_MEMCACHED)
            self.task_manager.add_new_task(task)
            self.task.jython_task_manager.get_task_result(task)
            sleep(5, "Wait for all docs to get ambiguous aborts")
            cb_err.revert(CouchbaseError.STOP_MEMCACHED)

            if len(task.fail.keys()) != num_items[task]:
                self.log.error("Failure not seen for few keys")
                result = False

            # All other doc_loading will succeed
            for load_gen in load_gens[1:]:
                task = load_docs(load_gen)
                tasks.append(task)
                num_items[task] = get_num_items(load_gen)
            for task in tasks:
                self.task_manager.get_task_result(task)
                if len(task.fail.keys()) != 0:
                    self.log.error("Errors during successful load attempt")
                    result = False
        elif load_type == "aborts_at_end":
            # All but last doc_loading will succeed
            for load_gen in load_gens[:-1]:
                task = load_docs(load_gen)
                tasks.append(task)
                num_items[task] = get_num_items(load_gen)
            for task in tasks:
                self.task_manager.get_task_result(task)
                if len(task.fail.keys()) != 0:
                    self.log.error("Errors during successful load attempt")
                    result = False

            # Task for trialling aborts
            task = load_docs(load_gens[-1], "abort")
            num_items[task] = get_num_items(load_gens[-1])
            cb_err.create(CouchbaseError.STOP_MEMCACHED)
            self.task_manager.add_new_task(task)
            self.task.jython_task_manager.get_task_result(task)
            sleep(5, "Wait for all docs to get ambiguous aborts")
            cb_err.revert(CouchbaseError.STOP_MEMCACHED)

            if len(task.fail.keys()) != num_items[task]:
                self.log.error("Failure not seen for few keys")
                result = False
        elif load_type == "mixed_aborts":
            for load_gen in load_gens:
                tasks.append(load_docs(load_gen, "abort"))
                self.task_manager.add_new_task(tasks[-1])

            # Loop to simulate periodic aborts wrt to given node
            tasks_completed = True
            while not tasks_completed:
                for task in tasks:
                    if not task.completed:
                        tasks_completed = False
                        break

                cb_err.create(CouchbaseError.STOP_MEMCACHED)
                sleep(3, "Wait to simulate random aborts")
                cb_err.revert(CouchbaseError.STOP_MEMCACHED)
            for task in tasks:
                self.task_manager.get_task_result(task)
        else:
            # Default is all_aborts
            for load_gen in load_gens:
                task = load_docs(load_gen, "abort")
                tasks.append(task)
                num_items[task] = get_num_items(load_gen)

            sleep(10, "Wait to avoid BUCKET_OPEN_IN_PROGRESS from SDK")
            cb_err.create(CouchbaseError.STOP_MEMCACHED)
            for task in tasks:
                self.task_manager.add_new_task(task)
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
            cb_err.revert(CouchbaseError.STOP_MEMCACHED)

            for task in tasks:
                if len(task.fail.keys()) != num_items[task]:
                    self.log.error("Failure not seen for few keys")
                    result = False

        return result

    def verify_unacked_bytes_all_buckets(self, cluster, filter_list=[]):
        """
        Waits for max_unacked_bytes = 0 on all servers and buckets in a cluster
        A utility function that waits upr flow with unacked_bytes = 0
        """
        servers = self.cluster_util.get_kv_nodes(cluster)
        dcp_stat_map = self.data_collector.collect_compare_dcp_stats(
            cluster.buckets, servers, filter_list=filter_list)
        for bucket in list(dcp_stat_map.keys()):
            if dcp_stat_map[bucket]:
                self.log.critical("Bucket {0} has unacked bytes != 0: {1}"
                                  .format(bucket, dcp_stat_map[bucket]))

    def disable_compaction(self, cluster, bucket="default"):
        new_config = {"viewFragmentThresholdPercentage": None,
                      "dbFragmentThresholdPercentage": None,
                      "dbFragmentThreshold": None,
                      "viewFragmentThreshold": None}
        self.modify_fragmentation_config(cluster, new_config, bucket)

    @staticmethod
    def modify_fragmentation_config(cluster, config, bucket="default"):
        bucket_op = BucketHelper(cluster.master)
        _config = {"parallelDBAndVC": "false",
                   "dbFragmentThreshold": None,
                   "viewFragmentThreshold": None,
                   "dbFragmentThresholdPercentage": 100,
                   "viewFragmentThresholdPercentage": 100,
                   "allowedTimePeriodFromHour": None,
                   "allowedTimePeriodFromMin": None,
                   "allowedTimePeriodToHour": None,
                   "allowedTimePeriodToMin": None,
                   "allowedTimePeriodAbort": None,
                   "autoCompactionDefined": "true"}
        _config.update(config)
        bucket_op.set_auto_compaction(
            bucket_name=bucket,
            parallelDBAndVC=_config["parallelDBAndVC"],
            dbFragmentThreshold=_config["dbFragmentThreshold"],
            viewFragmentThreshold=_config["viewFragmentThreshold"],
            dbFragmentThresholdPercentage=_config["dbFragmentThresholdPercentage"],
            viewFragmentThresholdPercentage=_config["viewFragmentThresholdPercentage"],
            allowedTimePeriodFromHour=_config["allowedTimePeriodFromHour"],
            allowedTimePeriodFromMin=_config["allowedTimePeriodFromMin"],
            allowedTimePeriodToHour=_config["allowedTimePeriodToHour"],
            allowedTimePeriodToMin=_config["allowedTimePeriodToMin"],
            allowedTimePeriodAbort=_config["allowedTimePeriodAbort"])

    def get_vbucket_seqnos(self, servers, buckets, skip_consistency=False,
                           per_node=True):
        """
        Method to get vbucket information from a cluster using cbstats
        """
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(
            buckets, servers, collect_vbucket=False,
            collect_vbucket_seqno=True, collect_vbucket_details=False,
            perNode=per_node)
        if not skip_consistency:
            new_vbucket_stats = self.compare_per_node_for_vbucket_consistency(
                new_vbucket_stats)
        return new_vbucket_stats

    def get_vbucket_seqnos_per_Node_Only(self, cluster, servers, buckets):
        """
        Method to get vbucket information from a cluster using cbstats
        """
        servers = self.cluster_util.get_kv_nodes(cluster, servers)
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(
            buckets, servers, collect_vbucket=False,
            collect_vbucket_seqno=True, collect_vbucket_details=False,
            perNode=True)
        self.compare_per_node_for_vbucket_consistency(new_vbucket_stats)
        return new_vbucket_stats

    def compare_vbucket_seqnos(self, cluster, prev_vbucket_stats,
                               servers, buckets,
                               perNode=False, compare="=="):
        """
            Method to compare vbucket information to a previously stored value
        """
        # if self.withMutationOps:
        #     compare = "<="
        comp_map = dict()
        comp_map["uuid"] = {'type': "string", 'operation': "=="}
        comp_map["abs_high_seqno"] = {'type': "long", 'operation': compare}
        comp_map["purge_seqno"] = {'type': "string", 'operation': compare}

        new_vbucket_stats = dict()
        self.log.debug("Begin Verification for vbucket seq_nos comparison")
        if perNode:
            new_vbucket_stats = self.get_vbucket_seqnos_per_Node_Only(
                cluster, servers, buckets)
        else:
            new_vbucket_stats = self.get_vbucket_seqnos(servers, buckets)
        if not perNode:
            compare_vbucket_seqnos_result = self.data_analyzer.compare_stats_dataset(
                prev_vbucket_stats, new_vbucket_stats, "vbucket_id",
                comparisonMap=comp_map)
            isNotSame, summary, _ = self.result_analyzer.analyze_all_result(
                compare_vbucket_seqnos_result, addedItems=False,
                deletedItems=False, updatedItems=False)
        else:
            compare_vbucket_seqnos_result = self.data_analyzer.compare_per_node_stats_dataset(
                prev_vbucket_stats, new_vbucket_stats, "vbucket_id",
                comparisonMap=comp_map)
            isNotSame, summary, _ = self.result_analyzer.analyze_per_node_result(
                compare_vbucket_seqnos_result, addedItems=False,
                deletedItems=False, updatedItems=False)
        if not isNotSame:
            raise Exception(summary)
        self.log.debug("End Verification for vbucket seq_nos comparison")
        return new_vbucket_stats

    @staticmethod
    def compare_per_node_for_vbucket_consistency(map1, check_abs_high_seqno=False,
                                                 check_purge_seqno=False):
        """
            Method to check uuid is consistent on active and replica new_vbucket_stats
        """
        bucketMap = dict()
        logic = True
        for bucket in list(map1.keys()):
            map = dict()
            nodeMap = dict()
            output = ""
            for node in list(map1[bucket].keys()):
                for vbucket in list(map1[bucket][node].keys()):
                    uuid = map1[bucket][node][vbucket]['uuid']
                    abs_high_seqno = map1[bucket][node][vbucket]['abs_high_seqno']
                    purge_seqno = map1[bucket][node][vbucket]['purge_seqno']
                    if vbucket in map.keys():
                        common_str = "\n bucket {0}, vbucket {1} :: " \
                                     "Original in node {2}. UUID" \
                            .format(bucket, vbucket, nodeMap[vbucket])
                        if map[vbucket]['uuid'] != uuid:
                            logic = False
                            output += "{0} {1}, Change in node {2}. UUID {3}" \
                                .format(common_str, map[vbucket]['uuid'],
                                        node, uuid)
                        if check_abs_high_seqno and int(map[vbucket]['abs_high_seqno']) != int(abs_high_seqno):
                            logic = False
                            output += "{0} {1}, Change in node {2}. UUID {3}" \
                                .format(common_str,
                                        map[vbucket]['abs_high_seqno'],
                                        node, abs_high_seqno)
                        if check_purge_seqno and int(map[vbucket]['purge_seqno']) != int(purge_seqno):
                            logic = False
                            output += "{0} {1}, Change in node {2}. UUID {3}" \
                                .format(common_str,
                                        map[vbucket]['abs_high_seqno'],
                                        node, abs_high_seqno)
                    else:
                        map[vbucket] = dict()
                        map[vbucket]['uuid'] = uuid
                        map[vbucket]['abs_high_seqno'] = abs_high_seqno
                        map[vbucket]['purge_seqno'] = purge_seqno
                        nodeMap[vbucket] = node
            bucketMap[bucket] = map
        if not logic:
            raise Exception(output)
        return bucketMap

    @staticmethod
    def compare_vbucketseq_failoverlogs(vbucketseq={}, failoverlog={}):
        """
            Method to compare failoverlog and vbucket-seq for uuid and  seq no
        """
        isTrue = True
        output = ""
        for bucket in list(vbucketseq.keys()):
            for vbucket in list(vbucketseq[bucket].keys()):
                seq = vbucketseq[bucket][vbucket]['abs_high_seqno']
                uuid = vbucketseq[bucket][vbucket]['uuid']
                fseq = failoverlog[bucket][vbucket]['seq']
                fuuid = failoverlog[bucket][vbucket]['id']
                common_str = "\n Error Condition in bucket {0} vbucket {1}::" \
                    .format(bucket, vbucket)
                if seq < fseq:
                    output += "{0} seq:vbucket-seq {1} != failoverlog-seq {2}" \
                        .format(common_str, seq, fseq)
                    isTrue = False
                if uuid != fuuid:
                    output += "{0} uuid : vbucket-seq {1} != failoverlog-seq {2}" \
                        .format(common_str, uuid, fuuid)
                    isTrue = False
        if not isTrue:
            raise Exception(output)

    @staticmethod
    def print_results_per_node(node_map):
        """ Method to print map results - Used only for debugging purpose """
        for bucket in list(node_map.keys()):
            print("----- Bucket {0} -----".format(bucket))
            for node in list(node_map[bucket].keys()):
                print("-------------Node {0}------------".format(node))
                for vbucket in list(node_map[bucket][node].keys()):
                    print("   for vbucket {0}".format(vbucket))
                    for key in list(node_map[bucket][node][vbucket].keys()):
                        print("            :: for key {0} = {1}"
                              .format(key,
                                      node_map[bucket][node][vbucket][key]))

    def vb_distribution_analysis(self, cluster,
                                 servers=[], buckets=[], num_replicas=0,
                                 std=1.0, type="rebalance",
                                 graceful=True):
        """
        Method to check vbucket distribution analysis after rebalance
        """
        self.log.debug("Begin Verification for vb_distribution_analysis")
        servers = self.cluster_util.get_kv_nodes(cluster, servers)
        active, replica = self.get_vb_distribution_active_replica(
            cluster, servers=servers, buckets=buckets)
        for bucket in buckets:
            self.log.debug("Begin Verification for Bucket {0}".format(bucket))
            active_result = active[bucket.name]
            replica_result = replica[bucket.name]
            if graceful or type == "rebalance":
                self.assertTrue(active_result["total"] == bucket.numVBuckets,
                                "total vbuckets do not match for active data set (= criteria), actual {0} expectecd {1}"
                                .format(active_result["total"], bucket.numVBuckets))
            else:
                self.assertTrue(active_result["total"] <= bucket.numVBuckets,
                                "total vbuckets do not match for active data set  (<= criteria), actual {0} expectecd {1}"
                                .format(active_result["total"], bucket.numVBuckets))
            if type == "rebalance":
                if (len(cluster.kv_nodes) - num_replicas) >= 1:
                    self.assertTrue(replica_result["total"] == num_replicas * bucket.numVBuckets,
                                    "total vbuckets do not match for replica data set (= criteria), actual {0} expected {1}"
                                    .format(replica_result["total"], num_replicas * bucket.numVBuckets))
                else:
                    self.assertTrue(replica_result["total"] < num_replicas * bucket.numVBuckets,
                                    "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}"
                                    .format(replica_result["total"], num_replicas * bucket.numVBuckets))
            else:
                self.assertTrue(replica_result["total"] <= num_replicas * bucket.numVBuckets,
                                "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}"
                                .format(replica_result["total"], num_replicas * bucket.numVBuckets))
            self.assertTrue(active_result["std"] >= 0.0 and active_result["std"] <= std,
                            "std test failed for active vbuckets")
            self.assertTrue(replica_result["std"] >= 0.0 and replica_result["std"] <= std,
                            "std test failed for replica vbuckets")
        self.log.debug("End Verification for vb_distribution_analysis")

    def data_analysis_active_replica_all(self, prev_data_set_active,
                                         prev_data_set_replica, servers,
                                         buckets, path=None,
                                         mode="disk"):
        """
            Method to do data analysis using cb transfer
            This works at cluster level
            1) Get Active and Replica data_path
            2) Compare Previous Active and Replica data
            3) Compare Current Active and Replica data
        """
        return True
        self.log.debug("Begin Verification for data comparison")
        info, curr_data_set_replica = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, getReplica=True,
            mode=mode)
        info, curr_data_set_active = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, getReplica=False,
            mode=mode)
        self.log.debug("Comparing:: Prev vs Current:: Active and Replica")
        comparison_result_replica = self.data_analyzer.compare_all_dataset(
            info, prev_data_set_replica, curr_data_set_replica)
        comparison_result_active = self.data_analyzer.compare_all_dataset(
            info, prev_data_set_active, curr_data_set_active)
        logic_replica, summary_replica, output_replica = self.result_analyzer.analyze_all_result(
            comparison_result_replica, deletedItems=False, addedItems=False,
            updatedItems=False)
        logic_active, summary_active, output_active = \
            self.result_analyzer.analyze_all_result(
                comparison_result_active, deletedItems=False, addedItems=False,
                updatedItems=False)
        if not logic_replica:
            self.log.error(output_replica)
            raise Exception(output_replica)
        if not logic_active:
            self.log.error(output_active)
            raise Exception(output_active)
        self.log.debug("Comparing :: Current :: Active and Replica")
        comparison_result = self.data_analyzer.compare_all_dataset(
            info, curr_data_set_active, curr_data_set_replica)
        logic, summary, output = self.result_analyzer.analyze_all_result(
            comparison_result, deletedItems=False, addedItems=False,
            updatedItems=False)
        self.log.debug("End Verification for data comparison")

    def data_analysis_all(self, prev_data_set, servers, buckets, path=None,
                          mode="disk", deletedItems=False, addedItems=False,
                          updatedItems=False):
        """
        Method to do data analysis using cb transfer.
        This works at cluster level
        """
        return True
        self.log.debug("Begin Verification for data comparison")
        servers = self.cluster_util.get_kv_nodes(cluster, servers)
        info, curr_data_set = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, mode=mode)
        comparison_result = self.data_analyzer.compare_all_dataset(
            info, prev_data_set, curr_data_set)
        logic, summary, output = self.result_analyzer.analyze_all_result(
            comparison_result, deletedItems=deletedItems,
            addedItems=addedItems, updatedItems=updatedItems)
        if not logic:
            raise Exception(summary)
        self.log.debug("End Verification for data comparison")

    def get_data_set_all(self, servers, buckets, path=None, mode="disk"):
        """ Method to get all data set for buckets and from the servers """
        return True
        servers = self.cluster_util.get_kv_nodes(servers)
        _, dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, mode=mode)
        return dataset

    def get_data_set_with_data_distribution_all(self, cluster, servers,
                                                buckets,
                                                path=None, mode="disk"):
        """ Method to get all data set for buckets and from the servers """
        servers = self.cluster_util.get_kv_nodes(cluster, servers)
        _, dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, mode=mode)
        distribution = self.data_analyzer.analyze_data_distribution(dataset)
        return dataset, distribution

    def get_vb_distribution_active_replica(self, cluster, servers=[],
                                           buckets=[]):
        """
        Method to distribution analysis for active and replica vbuckets
        """
        servers = self.cluster_util.get_kv_nodes(cluster, servers)
        active, replica = self.data_collector.collect_vbucket_num_stats(
            servers, buckets)
        active_result, replica_result = \
            self.data_analyzer.compare_analyze_active_replica_vb_nums(active,
                                                                      replica)
        return active_result, replica_result

    def get_and_compare_active_replica_data_set_all(self, servers, buckets,
                                                    path=None, mode="disk"):
        """
        Method to get all data set for buckets and from the servers
        1) Get active and replica data in the cluster
        2) Compare active and replica data in the cluster
        3) Return active and replica data
        """
        return True, True
        servers = self.cluster_util.get_kv_nodes(servers)
        _, disk_replica_dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, getReplica=True,
            mode=mode)
        info, disk_active_dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, getReplica=False,
            mode=mode)
        self.log.debug("Begin Verification for Active Vs Replica")
        comparison_result = self.data_analyzer.compare_all_dataset(
            info, disk_replica_dataset, disk_active_dataset)
        logic, summary, _ = self.result_analyzer.analyze_all_result(
            comparison_result, deletedItems=False, addedItems=False,
            updatedItems=False)
        if not logic:
            self.log.error(summary)
            raise Exception(summary)
        self.log.debug("End Verification for Active Vs Replica")
        return disk_replica_dataset, disk_active_dataset

    @staticmethod
    def compare_per_node_for_failovers_consistency(map1, vbucketMap):
        """
        Method to check uuid is consistent on active and
        replica new_vbucket_stats
        """
        bucketMap = dict()
        logic = True
        for bucket in list(map1.keys()):
            map = dict()
            tempMap = dict()
            output = ""
            for node in list(map1[bucket].keys()):
                for vbucket in list(map1[bucket][node].keys()):
                    id = map1[bucket][node][vbucket]['id']
                    seq = map1[bucket][node][vbucket]['seq']
                    num_entries = map1[bucket][node][vbucket]['num_entries']
                    state = vbucketMap[bucket][node][vbucket]['state']
                    if vbucket in map.keys():
                        if map[vbucket]['id'] != id:
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: UUID {4}, Change node {5} {6} UUID {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['id'], node, state, id)
                        if int(map[vbucket]['seq']) != int(seq):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: seq {4}, Change node {5} {6}  :: seq {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['seq'], node, state, seq)
                        if int(map[vbucket]['num_entries']) != int(num_entries):
                            logic = False
                            output += "\n bucket {0}, vbucket {1} :: Original node {2} {3} :: num_entries {4}, Change node {5} {6} :: num_entries {7}".format(
                                bucket, vbucket, tempMap[vbucket]['node'], tempMap[vbucket]['state'],
                                map[vbucket]['num_entries'], node, state, num_entries)
                    else:
                        map[vbucket] = dict()
                        tempMap[vbucket] = dict()
                        map[vbucket]['id'] = id
                        map[vbucket]['seq'] = seq
                        map[vbucket]['num_entries'] = num_entries
                        tempMap[vbucket]['node'] = node
                        tempMap[vbucket]['state'] = state
            bucketMap[bucket] = map
        if not logic:
            raise Exception(output)
        return bucketMap

    def get_failovers_logs(self, servers, buckets):
        """
        Method to get failovers logs from a cluster using cbstats
        """
        vbucketMap = self.data_collector.collect_vbucket_stats(
            buckets, servers, collect_vbucket=True,
            collect_vbucket_seqno=False, collect_vbucket_details=False,
            perNode=True)
        new_failovers_stats = self.data_collector.collect_failovers_stats(
            buckets, servers, perNode=True)
        new_failovers_stats = self.compare_per_node_for_failovers_consistency(
            new_failovers_stats, vbucketMap)
        return new_failovers_stats

    def compare_failovers_logs(self, cluster, prev_failovers_stats,
                               servers, buckets,
                               perNode=False, comp_map=None):
        """
        Method to compare failover log information to a previously stored value
        """
        comp_map = dict()
        comp_map["id"] = {'type': "string", 'operation': "=="}
        comp_map["seq"] = {'type': "long", 'operation': "<="}
        comp_map["num_entries"] = {'type': "string", 'operation': "<="}

        self.log.debug("Begin Verification for failovers logs comparison")
        servers = self.cluster_util.get_kv_nodes(cluster, servers)
        new_failovers_stats = self.get_failovers_logs(servers, buckets)
        compare_failovers_result = self.data_analyzer.compare_stats_dataset(
            prev_failovers_stats, new_failovers_stats, "vbucket_id", comp_map)
        isNotSame, summary, result = self.result_analyzer.analyze_all_result(
            compare_failovers_result, addedItems=False, deletedItems=False,
            updatedItems=False)
        if not isNotSame:
            raise Exception(summary)
        self.log.debug("End Verification for failovers logs comparison")
        return new_failovers_stats

    def sync_ops_all_buckets(self, cluster, docs_gen_map, batch_size=10,
                             verify_data=True, exp=0, num_items=0,
                             load_using="default_loader"):
        for key in list(docs_gen_map.keys()):
            if key != "remaining":
                op_type = key
                if key == "expiry":
                    op_type = "update"
                    verify_data = False
                    self.expiry = 3
                self.sync_load_all_buckets(cluster, docs_gen_map[key][0],
                                           op_type, exp, load_using=load_using)
                if verify_data:
                    self.verify_cluster_stats(cluster, num_items)
        if "expiry" in docs_gen_map.keys():
            self._expiry_pager(cluster)

    def async_ops_all_buckets(self, cluster, docs_gen_map={}, batch_size=10):
        tasks = []
        if "expiry" in docs_gen_map.keys():
            self._expiry_pager(cluster)
        for key in list(docs_gen_map.keys()):
            if key != "remaining":
                op_type = key
                if key == "expiry":
                    op_type = "update"
                    self.expiry = 3
                tasks += self.async_load(docs_gen_map[key], op_type=op_type,
                                         exp=self.expiry,
                                         batch_size=batch_size)
        return tasks

    def _expiry_pager(self, cluster, val=10):
        kv_nodes = self.cluster_util.get_kv_nodes(cluster)
        rest = BucketRestApi(kv_nodes[0])
        for node in kv_nodes:
            shell_conn = RemoteMachineShellConnection(node)
            cbepctl_obj = Cbepctl(shell_conn)
            for bucket in cluster.buckets:
                rest.edit_bucket(bucket.name, {"expiryPagerSleepTime": val})
                cbepctl_obj.set(bucket.name,
                                "flush_param",
                                "exp_pager_initial_run_time",
                                "disable")
            shell_conn.disconnect()

    def _compaction_exp_mem_threshold(self, cluster, val=100):
        for node in self.cluster_util.get_kv_nodes(cluster):
            shell_conn = RemoteMachineShellConnection(node)
            cbepctl_obj = Cbepctl(shell_conn)
            for bucket in cluster.buckets:
                cbepctl_obj.set(bucket.name,
                                "flush_param",
                                "compaction_exp_mem_threshold",
                                val)
            shell_conn.disconnect()

    def _run_compaction(self, cluster, number_of_times=1,
                        async_run=False, timeout=300):
        compaction_tasks = dict()
        for _ in range(number_of_times):
            for bucket in cluster.buckets:
                compaction_tasks[bucket.name] = self.task.async_compact_bucket(
                    cluster.master, bucket, timeout)
            if not async_run:
                for task in compaction_tasks.values():
                    self.task_manager.get_task_result(task)
        return compaction_tasks

    @staticmethod
    def get_item_count_mc(server, bucket):
        client = MemcachedClientHelper.direct_client(server, bucket)
        client.close()
        return int(client.stats()["curr_items"])

    @staticmethod
    def get_buckets_item_count(cluster, bucket_name=None,
                               exclude_system_scope=True):
        bucket_map = dict()
        bucket_rest = BucketRestApi(cluster.master)
        status, json_parsed = bucket_rest.get_bucket_info(
            bucket_name=bucket_name, basic_stats=True)
        if status is False:
            return bucket_map

        if bucket_name:
            # Because we directly got the bucket_specific dict here
            json_parsed = [json_parsed]

        param = [{
            "applyFunctions": ["sum"],
            "metric": [{"label": "bucket", "value": "%s"},
                       {"label": "scope", "value": "_system"},
                       {"label": "name", "value": "kv_collection_item_count"}],
            "nodesAggregation": "sum",
            "start": -1, "step": 1, "timeWindow": 1}]

        for item in json_parsed:
            b_name = item['name']
            bucket_map[b_name] = item['basicStats']['itemCount']
            if exclude_system_scope:
                # Replace a current bucket_name for fetching the right stat
                param[0]["metric"][0]["value"] = b_name
                status, range_stats = bucket_rest.get_stats_range(params=param)
                if status:
                    bucket_map[b_name] -= int(
                        range_stats[0]['data'][0]["values"][0][1])

        if bucket_name:
            return bucket_map[bucket_name]
        return bucket_map

    @staticmethod
    def expire_pager(servers, buckets, val=10):
        for bucket in buckets:
            for server in servers:
                ClusterOperationHelper.flushctl_set(server, "exp_pager_stime",
                                                    val, bucket)
        sleep(val, "Wait for expiry pager to run on all these nodes")

    def set_auto_compaction(self, bucket_name, bucket_helper,
                            parallelDBAndVC="false",
                            dbFragmentThreshold=None,
                            viewFragmentThreshold=None,
                            dbFragmentThresholdPercentage=None,
                            viewFragmentThresholdPercentage=None,
                            allowedTimePeriodFromHour=None,
                            allowedTimePeriodFromMin=None,
                            allowedTimePeriodToHour=None,
                            allowedTimePeriodToMin=None,
                            allowedTimePeriodAbort=None):
        output, rq_content = bucket_helper.set_auto_compaction(
            bucket_name,
            parallelDBAndVC, dbFragmentThreshold, viewFragmentThreshold,
            dbFragmentThresholdPercentage, viewFragmentThresholdPercentage,
            allowedTimePeriodFromHour, allowedTimePeriodFromMin,
            allowedTimePeriodToHour, allowedTimePeriodToMin,
            allowedTimePeriodAbort)

        if not output and (dbFragmentThresholdPercentage, dbFragmentThreshold,
                           viewFragmentThresholdPercentage,
                           viewFragmentThreshold <= MIN_COMPACTION_THRESHOLD
                           or dbFragmentThresholdPercentage,
                           viewFragmentThresholdPercentage >= MAX_COMPACTION_THRESHOLD):
            self.assertFalse(output,
                             "Should be impossible to set compaction val {0}%"
                             .format(viewFragmentThresholdPercentage))
            self.assertTrue("errors" in rq_content,
                            "Error is not present in response")
            self.assertTrue(str(rq_content["errors"])
                            .find("Allowed range is 2 - 100") > -1,
                            "Error 'Allowed range is 2 - 100', but was '{0}'"
                            .format(str(rq_content["errors"])))
            self.log.debug("Response contains error = '%(errors)s' as expected"
                           % rq_content)
        return output, rq_content

    @staticmethod
    def get_bucket_priority(priority):
        if priority is None:
            return None
        if priority.lower() == 'low':
            return None
        else:
            return priority

    def wait_for_vbuckets_ready_state(self, node, bucket,
                                      timeout_in_seconds=300, log_msg='',
                                      admin_user='cbadminbucket',
                                      admin_pass='password'):
        start_time = time.time()
        end_time = start_time + timeout_in_seconds
        ready_vbuckets = dict()
        rest = ClusterRestAPI(node)
        bucket_conn = BucketHelper(node)
        bucket_conn.vbucket_map_ready(bucket, 60)
        vbucket_count = len(bucket.vbuckets)
        vbuckets = bucket.vbuckets
        obj = VBucketAwareMemcached(rest, bucket, self.cluster_util, info=node)
        _, vbucket_map, _ = obj.request_map(rest, bucket)
        # Create dictionary with key:"ip:port" and value: a list of vbuckets
        server_dict = defaultdict(list)
        for everyID in range(0, vbucket_count):
            memcached_ip_port = str(vbucket_map[everyID])
            server_dict[memcached_ip_port].append(everyID)
        while time.time() < end_time and len(ready_vbuckets) < vbucket_count:
            for every_ip_port in server_dict:
                # Retrieve memcached ip and port
                ip, port = every_ip_port.split(":")
                client = mc_bin_client.MemcachedClient(ip, int(port),
                                                       timeout=30)
                client.vbucket_count = len(vbuckets)
                # versions = rest.get_nodes_versions(logging=False)
                client.sasl_auth_plain(admin_user, admin_pass)
                bucket_name = bucket.name.encode('ascii')
                client.bucket_select(bucket_name)
                for i in server_dict[every_ip_port]:
                    try:
                        (_, _, c) = client.get_vbucket_state(i)
                    except mc_bin_client.MemcachedError as e:
                        ex_msg = str(e)
                        if "Not my vbucket" in log_msg:
                            log_msg = log_msg[:log_msg.find("vBucketMap") + 12] + "..."
                        if e.status == memcacheConstants.ERR_NOT_MY_VBUCKET:
                            # May receive while waiting for vbuckets, retry
                            continue
                        self.log.error("%s: %s" % (log_msg, ex_msg))
                        continue
                    except EOFError:
                        # The client was disconnected for some reason. This can
                        # happen just after the bucket REST API is returned
                        # (before the buckets are created in each of the
                        # memcached processes)
                        # See here for some details:
                        # http://review.couchbase.org/#/c/49781/
                        # Longer term when we don't disconnect clients in this
                        # state we should probably remove this code.
                        self.log.error("Reconnecting to the server")
                        client.reconnect()
                        continue

                    c = c.decode()
                    if c.find("\x01") > 0 or c.find("\x02") > 0:
                        ready_vbuckets[i] = True
                    elif i in ready_vbuckets:
                        self.log.warning("vbucket state changed from active "
                                         "to {0}".format(c))
                        del ready_vbuckets[i]
                client.close()
        return len(ready_vbuckets) == vbucket_count

    # try to insert key in all vbuckets before returning from this function
    # bucket { 'name' : 90,'password':,'port':1211'}
    def wait_for_memcached(self, node, bucket, timeout_in_seconds=300,
                           log_msg=''):
        msg = "Waiting for memcached bucket: {0} in {1} to accept set ops"
        self.log.debug(msg.format(bucket, node.ip))
        all_vbuckets_ready = self.wait_for_vbuckets_ready_state(
            node, bucket, timeout_in_seconds, log_msg)
        # return (counter == vbucket_count) and all_vbuckets_ready
        return all_vbuckets_ready

    def print_dataStorage_content(self, servers):
        """"printout content of data and index path folders"""
        # Determine whether its a cluster_run/not
        cluster_run = True

        firstIp = servers[0].ip
        if len(servers) == 1 and servers[0].port == '8091':
            cluster_run = False
        else:
            for node in servers:
                if node.ip != firstIp:
                    cluster_run = False
                    break

        for serverInfo in servers:
            node = RestConnection(serverInfo).get_nodes_self()
            paths = set([node.storage[0].path, node.storage[0].index_path])
            for path in paths:
                if "c:/Program Files" in path:
                    path = path.replace("c:/Program Files",
                                        "/cygdrive/c/Program Files")

                if cluster_run:
                    call(["ls", "-lR", path])
                else:
                    self.log.debug(
                        "Total number of files. No need to printout all "
                        "that flood the test log.")
                    shell = RemoteMachineShellConnection(serverInfo)
                    # o, r = shell.execute_command("ls -LR '{0}'".format(path))
                    o, r = shell.execute_command("wc -l '{0}'".format(path))
                    shell.log_command_output(o, r)
                    shell.disconnect()

    def load_buckets_with_high_ops(self, server, bucket, items, batch=2000,
                                   threads=5, start_document=0, instances=1,
                                   ttl=0):
        import subprocess
        cmd_format = "python utils/bucket_utils/thanosied.py " \
                     "--spec couchbase://{0} --bucket {1} " \
                     "--user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} " \
                     "--start_document {7} --cb_version {8} --workers {9} " \
                     "--ttl {10} --rate_limit {11} " \
                     "--passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(
            server.ip, bucket.name, server.rest_username,
            server.rest_password, items, batch, threads, start_document,
            cb_version, instances, ttl, self.rate_limit)
        self.log.debug("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            # self.log.error(error)
            if "Authentication failed" in error:
                cmd = cmd_format.format(
                    server.ip, bucket.name, server.rest_username,
                    server.rest_password, items, batch, threads,
                    start_document, "4.0", instances, ttl, self.rate_limit)
                self.log.debug("Running {}".format(cmd))
                result = subprocess.Popen(cmd, shell=True,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE)
                output = result.stdout.read()
                error = result.stderr.read()
                if error:
                    self.log.error(error)
                    self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, items,
                             "Failed to load {} items. Loaded only {} items"
                             .format(items, total_loaded))

    def delete_buckets_with_high_ops(self, server, bucket, items, ops,
                                     batch=20000, threads=5,
                                     start_document=0, instances=1):
        import subprocess
        cmd_format = "python utils/bucket_utils/thanosied.py " \
                     "--spec couchbase://{0} --bucket {1} " \
                     "--user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} " \
                     "--start_document {7} --cb_version {8} " \
                     "--workers {9} --rate_limit {10} " \
                     "--passes 1  --delete --num_delete {4}"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if self.num_replicas > 0 and self.use_replica_to:
            cmd_format = "{} --replicate_to 1".format(cmd_format)
        cmd = cmd_format.format(
            server.ip, bucket.name, server.rest_username,
            server.rest_password, items, batch, threads, start_document,
            cb_version, instances, ops)
        self.log.debug("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        output = result.stdout.read()
        error = result.stderr.read()
        if error:
            self.log.error(error)
            self.fail("Failed to run the loadgen.")
        if output:
            loaded = output.split('\n')[:-1]
            total_loaded = 0
            for load in loaded:
                total_loaded += int(load.split(':')[1].strip())
            self.assertEqual(total_loaded, ops,
                             "Failed to update {} items. Loaded {} items"
                             .format(ops, total_loaded))

    def check_dataloss_for_high_ops_loader(self, server, bucket, items,
                                           batch=2000, threads=5,
                                           start_document=0,
                                           updated=False, ops=0, ttl=0,
                                           deleted=False, deleted_items=0):
        import subprocess
        cmd_format = "python utils/bucket_utils/thanosied.py " \
                     "--spec couchbase://{0} --bucket {1} " \
                     "--user {2} --password {3} " \
                     "--count {4} --batch_size {5} --threads {6} " \
                     "--start_document {7} --cb_version {8} --validation 1 " \
                     "--rate_limit {9} --passes 1"
        cb_version = RestConnection(server).get_nodes_version()[:3]
        if updated:
            cmd_format = "{} --updated --ops {}".format(cmd_format, ops)
        if deleted:
            cmd_format = "{} --deleted --deleted_items {}" \
                .format(cmd_format, deleted_items)
        if ttl > 0:
            cmd_format = "{} --ttl {}".format(cmd_format, ttl)
        cmd = cmd_format.format(server.ip, bucket.name, server.rest_username,
                                server.rest_password, int(items), batch,
                                threads, start_document, cb_version,
                                self.rate_limit)
        self.log.debug("Running {}".format(cmd))
        result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        _ = result.stdout.read()
        errors = result.stderr.read()
        return errors

    def get_all_buckets(self, cluster, cluster_node=None):
        node = cluster_node or cluster.master
        bucket_rest = BucketRestApi(node)
        _, json_parsed = bucket_rest.get_bucket_info()
        bucket_list = list()
        for item in json_parsed:
            bucket_list.append(self.parse_get_bucket_json(cluster.buckets,
                                                          item))
        return bucket_list

    def fetch_rank_map(self, cluster, cluster_node=None,
                       fetch_latest_buckets=False):
        rank_map = {}
        if cluster_node or fetch_latest_buckets:
            if cluster_node is None:
                cluster_node = cluster.master
            rest = BucketHelper(cluster_node)
            json_parsed = rest.get_buckets_json()
            for item in json_parsed:
                if Bucket.rank in item:
                    rank_map[item[Bucket.name]] = item[Bucket.rank]
                else:
                    rank_map[item[Bucket.name]] = None
        else:
            rank_map = {bucket.name : bucket.rank for bucket in cluster.buckets}
        return rank_map

    @staticmethod
    def get_fragmentation_kv(cluster, bucket=None, server=None):
        if bucket is None:
            bucket = cluster.buckets[0]
        if server is None:
            server = cluster.master
        bucket_helper = BucketHelper(server)
        stats = bucket_helper.fetch_bucket_stats(bucket.name)
        return float(stats["op"]["samples"]["couch_docs_fragmentation"][-1])

    def parse_get_bucket_json(self, buckets, parsed):
        for bucket in buckets:
            if bucket.name == parsed[Bucket.name]:
                break
        else:
            bucket = Bucket()

            # Following params can be initialized only once,
            # so avoiding overwriting of already known values to enhance
            # validation procedures. And these values has to be updated
            # only by the update_bucket_props() to track the current value
            bucket.name = parsed[Bucket.name]
            if Bucket.numVBuckets in parsed:
                bucket.numVBuckets = parsed[Bucket.numVBuckets]

            if Bucket.warmupBehavior in parsed:
                bucket.warmupBehavior = parsed[Bucket.warmupBehavior]

            if Bucket.fusionLogstoreURI in parsed:
                bucket.fusionLogstoreURI = parsed[Bucket.fusionLogstoreURI]

            bucket.bucketType = parsed[Bucket.bucketType]
            if Bucket.maxTTL in parsed:
                bucket.maxTTL = parsed[Bucket.maxTTL]

            if Bucket.width in parsed:
                bucket.serverless = Serverless()
                bucket.serverless.width = parsed[Bucket.width]
                bucket.serverless.weight = parsed[Bucket.weight]

            if Bucket.durabilityMinLevel in parsed:
                bucket.durabilityMinLevel = parsed[Bucket.durabilityMinLevel]

            if Bucket.rank in parsed:
                bucket.rank = parsed[Bucket.rank]

            if Bucket.compressionMode in parsed:
                bucket.compressionMode = parsed[Bucket.compressionMode]

            if Bucket.conflictResolutionType in parsed:
                bucket.conflictResolutionType = \
                    parsed[Bucket.conflictResolutionType]

            if Bucket.rank in parsed:
                bucket.rank = parsed[Bucket.rank]

        # Sanitise the value to the expected value for cb buckets
        if bucket.bucketType == 'membase':
            bucket.bucketType = Bucket.Type.MEMBASE

        # Set only if not set previously
        # (Can happen since we directly adding the bucket_obj to the cluster)
        bucket.uuid = bucket.uuid or parsed[Bucket.uuid]
        bucket.bucketCapabilities = parsed["bucketCapabilities"]

        if 'vBucketServerMap' in parsed:
            vBucketServerMap = parsed['vBucketServerMap']
            serverList = vBucketServerMap['serverList']
            if "numReplicas" in vBucketServerMap:
                bucket.replicaNumber = vBucketServerMap["numReplicas"]
            # vBucketMapForward
            if 'vBucketMapForward' in vBucketServerMap:
                # let's gather the forward map
                vBucketMapForward = vBucketServerMap['vBucketMapForward']
                counter = 0
                for vbucket in vBucketMapForward:
                    # there will be n number of replicas
                    vbucketInfo = Bucket.vBucket()
                    vbucketInfo.master = serverList[vbucket[0]]
                    if vbucket:
                        for i in range(1, len(vbucket)):
                            if vbucket[i] != -1:
                                vbucketInfo.replica.append(
                                    serverList[vbucket[i]])
                    vbucketInfo.id = counter
                    counter += 1
                    bucket.forward_map.append(vbucketInfo)
            vBucketMap = vBucketServerMap['vBucketMap']
            counter = 0
            for vbucket in vBucketMap:
                # there will be n number of replicas
                vbucketInfo = Bucket.vBucket()
                vbucketInfo.master = serverList[vbucket[0]]
                if vbucket:
                    for i in range(1, len(vbucket)):
                        if vbucket[i] != -1:
                            vbucketInfo.replica.append(serverList[vbucket[i]])
                vbucketInfo.id = counter
                counter += 1
                bucket.vbuckets.append(vbucketInfo)
        stats = parsed['basicStats']
        self.log.debug('Stats: {0}'.format(stats))
        bucket.stats.opsPerSec = stats['opsPerSec']
        bucket.stats.itemCount = stats['itemCount']
        if bucket.bucketType != "memcached":
            bucket.stats.diskUsed = stats['diskUsed']
        bucket.stats.memUsed = stats['memUsed']
        bucket.stats.ram = parsed['quota']['ram']
        return bucket

    def wait_till_total_numbers_match(self, cluster, bucket,
                                      timeout_in_seconds=120,
                                      num_zone=1):
        self.log.debug('Waiting for sum_of_curr_items == total_items')
        start = time.time()
        verified = False
        while (time.time() - start) <= timeout_in_seconds:
            try:
                if self.verify_items_count(cluster, bucket, num_zone=num_zone):
                    verified = True
                    break
                else:
                    sleep(2)
            except StatsUnavailableException:
                self.log.error("Unable to retrieve stats for any node!")
                break
        if not verified:
            rest = RestConnection(cluster.master)
            RebalanceHelper.print_taps_from_all_nodes(rest, bucket)
        return verified

    def verify_vbucket_distribution_in_zones(self, cluster, nodes, servers):
        """
        Verify the active and replica distribution in nodes in same zones.
        Validate that no replicas of a node are in the same zone.
        :param buckets: target bucket objects
        :param nodes: Map of the nodes in different zones.
        Each key contains the zone name and the ip of nodes in that zone.
        """
        buckets = self.get_all_buckets(cluster)
        for bucket in buckets:
            for group in nodes:
                vb_active_list = list()
                vb_replica_list = list()
                for node in nodes[group]:
                    for server in servers:
                        if server.ip == node:
                            cbstat = Cbstats(server)
                            vb_active_list.extend(cbstat.vbucket_list(bucket,
                                                                      "active"))
                            vb_replica_list.extend(cbstat.vbucket_list(bucket,
                                                                       "replica"))
                            cbstat.disconnect()
                            break
                if set(vb_active_list).isdisjoint(set(vb_replica_list)):
                    self.log.debug("Active and replica vbucket list"
                                   "are not overlapped")
                else:
                    raise Exception("Active and replica vbucket list"
                                    "are overlapped")

    def get_actual_replica(self, cluster, bucket, num_zone):
        servers = self.cluster_util.get_kv_nodes(cluster)
        available_replicas = bucket.replicaNumber
        if num_zone > 1:
            """
            replica number will be either replica number or one less than the zone number
            or min number of nodes in the zone
            zone =2:
                1 node each, replica set =1, actual =1
                1 node each, replica set >1, actual = 1
                2 nodes each, replica set =2, actual =2
                2 nodes each, replica set =3, actual =2
                3 nodes each, replica set =3, actual =3
                group1: 2 nodes, group 1: 1 node, replica_set >1, actual = 1
                group1: 3 nodes, group 1: 2 node, replica_set >=2, actual = 2
                group1: 4 nodes, group 1: 5 node, replica_set =3, actual = 3

            zone =3:
                1 node each, replica_set=2, actual =2
                2 nodes each, relica_set =3, actual =3
                3 nodes each,repica_set = 3, actaul = 3
                num_node_in_each_group: [1, 2, 1], replica_set=3, actual = 2
                num_node_in_each_group: [2, 2, 1], replica_set=3, actual = 3
                num_node_in_each_group: [3, 2, 1], replica_set=3, actual = 3
                num_node_in_each_group: [3, 3, 1], replica_set=3, actual = 3
            """
            if bucket.replicaNumber >= num_zone:
                available_replicas = self.get_min_nodes_in_zone(cluster)
                if available_replicas > bucket.replicaNumber:
                    available_replicas = bucket.replicaNumber
            self.log.info(
                "available_replicas considered {0}".format(available_replicas))
        elif len(servers) <= bucket.replicaNumber:
            available_replicas = len(servers) - 1
        return available_replicas

    def verify_items_count(self, cluster, bucket, num_attempt=3, timeout=2,
                           num_zone=1):
        # get the #of buckets from rest
        vbucket_active_sum = 0
        vbucket_replica_sum = 0
        vbucket_pending_sum = 0
        all_server_stats = []
        stats_received = True
        nodes = global_vars.cluster_util.get_nodes(cluster.master)
        cbstat = Cbstats(cluster.master)
        bucket_helper = BucketHelper(cluster.master)
        active_vbucket_differ_count = len(nodes)
        replica_factor = self.get_actual_replica(cluster, bucket, num_zone)
        for server in nodes:
            # get the stats
            server_stats = bucket_helper.get_bucket_stats_for_node(
                bucket.name, server)
            if not server_stats:
                self.log.debug("Unable to get stats from {0}: {1}"
                               .format(server.ip, server.port))
                stats_received = False
            all_server_stats.append((server, server_stats))
        if not stats_received:
            raise StatsUnavailableException()
        curr_items = 0
        max_vbuckets = int(cbstat.all_stats(bucket.name)["ep_max_vbuckets"])
        master_stats = bucket_helper.get_bucket_stats(bucket.name)
        cbstat.disconnect()
        if "vb_active_num" in master_stats:
            self.log.debug('vb_active_num from master: {0}'
                           .format(master_stats["vb_active_num"]))
        else:
            raise Exception("Bucket {0} stats doesnt contain 'vb_active_num':"
                            .format(bucket))
        for server, single_stats in all_server_stats:
            if not single_stats or "curr_items" not in single_stats:
                continue
            curr_items += single_stats["curr_items"]
            self.log.debug("curr_items from {0}:{1} - {2}"
                           .format(server.ip, server.port,
                                   single_stats["curr_items"]))
            if 'vb_pending_num' in single_stats:
                vbucket_pending_sum += single_stats['vb_pending_num']
                self.log.debug("vb_pending_num from {0}:{1} - {2}"
                               .format(server.ip, server.port,
                                       single_stats["vb_pending_num"]))
            if 'vb_active_num' in single_stats:
                vbucket_active_sum += single_stats['vb_active_num']
                if (master_stats["vb_active_num"] + active_vbucket_differ_count) \
                        <= single_stats['vb_active_num'] <= \
                        (master_stats["vb_active_num"] - active_vbucket_differ_count):
                    raise Exception("vb_active_num from {0}:{1} - {2} "
                                    "and master {3}"
                                    .format(server.ip, server.port,
                                            single_stats["vb_active_num"],
                                            master_stats["vb_active_num"]))
                self.log.debug("vb_active_num from {0}:{1} - {2}"
                               .format(server.ip, server.port,
                                       single_stats["vb_active_num"]))
            if 'vb_replica_num' in single_stats:
                vbucket_replica_sum += single_stats['vb_replica_num']
                self.log.debug("vb_replica_num from {0}:{1} - {2}"
                               .format(server.ip, server.port,
                                       single_stats["vb_replica_num"]))

        msg = "sum of vb_active_num {0}, vb_pending_num {1}, vb_replica_num {2}"
        self.log.debug(msg.format(vbucket_active_sum, vbucket_pending_sum,
                                  vbucket_replica_sum))
        msg = 'sum: {0} and sum * (replica_factor + 1) ({1}) : {2}'
        self.log.debug(msg.format(curr_items, replica_factor + 1,
                                  (curr_items * (replica_factor + 1))))
        if "curr_items_tot" in master_stats:
            self.log.debug('curr_items_tot from master: {0}'
                           .format(master_stats["curr_items_tot"]))
        else:
            raise Exception("Bucket {0} stats doesnt contain 'curr_items_tot':"
                            .format(bucket))

        expected_replica_vbucket = max_vbuckets * replica_factor
        delta = curr_items * (replica_factor + 1) - master_stats["curr_items_tot"]
        if vbucket_active_sum != max_vbuckets:
            raise Exception("vbucket_active_sum actual {0} and expected {1}"
                            .format(vbucket_active_sum, max_vbuckets))
        elif vbucket_replica_sum != expected_replica_vbucket:
            raise Exception("vbucket_replica_sum actual {0} and expected {1}"
                            .format(vbucket_replica_sum,
                                    expected_replica_vbucket))
        else:
            self.log.debug('vbucket_active_sum: {0}'
                           'vbucket_replica_sum: {1}'
                           .format(vbucket_active_sum,
                                   vbucket_replica_sum))
        delta = abs(delta)

        if delta > 0:
            if curr_items == 0:
                missing_percentage = 0
            else:
                missing_percentage = delta * 1.0 / (curr_items * (replica_factor + 1))
            self.log.debug("Nodes stats are: {0}"
                           .format([node.ip for node in nodes]))
        else:
            missing_percentage = 1
        self.log.debug("Delta: {0} missing_percentage: {1} replica_factor: {2}"
                       .format(delta, missing_percentage, replica_factor))
        # If no items missing then, return True
        if not delta:
            return True
        return False

    def _wait_warmup_completed(self, bucket, servers=None, wait_time=300, check_for_persistence=True):
        # Return True, if bucket_type is not equal to MEMBASE
        if bucket.bucketType != Bucket.Type.MEMBASE:
            return True

        servers = servers or bucket.servers
        self.log.debug("Waiting for bucket %s to complete warm-up on nodes %s"
                       % (bucket.name, servers))

        warmed_up = False
        ready_for_persistence = False
        start = time.time()
        for server in servers:
            # Cbstats implementation to wait for bucket warmup
            warmed_up = False
            cbstat_obj = Cbstats(server)
            while time.time() - start < wait_time:
                try:
                    result = cbstat_obj.all_stats(bucket.name)
                    if result["ep_warmup_thread"] == "complete":
                        warmed_up = True
                        break
                except Exception as e:
                    self.log.warning("Exception during cbstat all cmd: %s" % e)
                sleep(2, "Warm-up not complete for %s on %s" % (bucket.name,
                                                                server.ip))
            ready_for_persistence = False
            if not check_for_persistence or bucket.storageBackend == Bucket.StorageBackend.couchstore:
                ready_for_persistence = True
                continue

            while time.time() - start < wait_time:
                try:
                    result = cbstat_obj.vbucket_seqno(bucket.name)
                    all_vbuckets_ready = True
                    for vbucket in result:
                        if int(result[vbucket]["last_persisted_seqno"]) == 0:
                            all_vbuckets_ready = False
                    if all_vbuckets_ready:
                        ready_for_persistence = True
                        break
                except Exception as e:
                    self.log.warning("Exception during cbstat vbucket-seqno cmd: {}".format(e))
                sleep(2, "Bucket {} is not ready for persistence on {}".format(bucket.name,
                                                                               server.ip))
            cbstat_obj.disconnect()
        return warmed_up and ready_for_persistence

    def add_rbac_user(self, cluster_node, testuser=None, rolelist=None):
        """
           From spock, couchbase server is built with some users that handles
           some specific task such as:
               cbadminbucket
           Default added user is cbadminbucket with admin role
        """
        # rest = BucketHelper(node)
        # cluster_compatibility = rest.check_cluster_compatibility("5.0")
        # if cluster_compatibility is None:
        #     pre_spock = True
        # else:
        #     pre_spock = not cluster_compatibility
        # if pre_spock:
        #     self.log.info("Atleast one of the nodes in the cluster is "
        #                   "pre 5.0 version. Hence not creating rbac user "
        #                   "for the cluster. RBAC is a 5.0 feature.")
        #     return
        if testuser is None:
            testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'password': 'password'}]
        if rolelist is None:
            rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'roles': 'admin'}]

        self.log.debug("**** Add built-in '%s' user to node %s ****"
                       % (testuser[0]["name"], cluster_node.ip))
        RbacUtil().create_user_source(testuser, 'builtin', cluster_node)

        self.log.debug("**** Add '%s' role to '%s' user ****"
                       % (rolelist[0]["roles"], testuser[0]["name"]))
        status = RbacUtil().add_user_role(rolelist,
                                          SecurityRestAPI(cluster_node),
                                          'builtin')
        return status

    def async_create_views(self, server, design_doc_name, views,
                           bucket="default", with_query=True,
                           check_replication=False):
        tasks = []
        if len(views):
            for view in views:
                t_ = self.async_create_view(
                    server, design_doc_name, view, bucket, with_query,
                    check_replication=check_replication)
                tasks.append(t_)
                sleep(0.1, "To make sure create_view task to get unique name",
                      log_type="infra")
        else:
            t_ = self.async_create_view(
                server, design_doc_name, None, bucket, with_query,
                check_replication=check_replication)
            tasks.append(t_)
        return tasks

    def create_views(self, server, design_doc_name, views, bucket="default",
                     timeout=None, check_replication=False):
        if len(views):
            for view in views:
                self.create_view(
                    server, design_doc_name, view, bucket, timeout,
                    check_replication=check_replication)
        else:
            self.create_view(
                server, design_doc_name, None, bucket, timeout,
                check_replication=check_replication)

    def async_create_view(self, server, design_doc_name, view,
                          bucket="default", with_query=True,
                          check_replication=False, ddoc_options=None):
        """
        Asynchronously creates a views in a design doc

        Parameters:
          server - The server to handle create view task. (TestInputServer)
          design_doc_name - Design doc to be created or updated with view(s)
                            being created (String)
          view - The view being created (document.View)
          bucket - Name of the bucket containing items for this view. (String)
          with_query - Wait indexing to get view query results after creation
          check_replication - Should the test check replication or not (Bool)
          ddoc_options - DDoc options to define automatic index building
                         (minUpdateChanges, updateInterval ...) (Dict)
        Returns:
          ViewCreateTask - A task future that is a handle to the scheduled task
        """
        _task = ViewCreateTask(
            server, design_doc_name, view, bucket, with_query,
            check_replication, ddoc_options)
        self.task_manager.add_new_task(_task)
        return _task

    def create_view(self, server, design_doc_name, view, bucket="default",
                    timeout=None, with_query=True, check_replication=False):
        """
        Synchronously creates a views in a design doc

        Parameters:
          server - The server to handle create view task. (TestInputServer)
          design_doc_name - Design doc to be created or updated with view(s)
                            being created (String)
          view - The view being created (document.View)
          bucket - Name of the bucket containing items for this view. (String)
          with_query - Wait indexing to get view query results after creation

        Returns:
          string - revision number of design doc
        """
        _task = self.async_create_view(server, design_doc_name, view, bucket,
                                       with_query, check_replication)
        return self.task_manager.get_task_result(_task)

    def async_delete_view(self, server, design_doc_name, view,
                          bucket="default"):
        """
        Asynchronously deletes a views in a design doc

        Parameters:
          server - The server to handle delete view task. (TestInputServer)
          design_doc_name - Design doc to be deleted or updated with view(s)
                            being deleted (String)
          view - The view being deleted (document.View)
          bucket - Name of the bucket containing items for this view. (String)

        Returns:
          ViewDeleteTask - A task future that is a handle to the scheduled task
        """
        _task = ViewDeleteTask(server, design_doc_name, view, bucket)
        self.task_manager.add_new_task(_task)
        return _task

    def delete_view(self, server, design_doc_name, view, bucket="default",
                    timeout=None):
        """
        Synchronously deletes a views in a design doc

        Parameters:
          server - The server to handle delete view task. (TestInputServer)
          design_doc_name - Design doc to be deleted or updated with view(s)
                            being deleted (String)
          view - The view being deleted (document.View)
          bucket - Name of the bucket containing items for this view. (String)

        Returns:
          boolean - Whether or not delete view was successful
        """
        _task = self.async_delete_view(server, design_doc_name, view, bucket)
        return self.task_manager.get_task_result(_task)

    def async_query_view(self, server, design_doc_name, view_name, query,
                         expected_rows=None, bucket="default", retry_time=2):
        """
        Asynchronously query a views in a design doc

        Parameters:
          server - The server to handle query view task. (TestInputServer)
          design_doc_name - Design doc with view(s) being queried(String)
          view_name - The view being queried (String)
          expected_rows - The number of rows expected to be returned from
                          the query (int)
          bucket - Name of the bucket containing items for this view. (String)
          retry_time - Time in secs to wait before retrying
                       failed queries (int)

        Returns:
          ViewQueryTask - A task future that is a handle to the scheduled task
        """
        _task = ViewQueryTask(server, design_doc_name, view_name, query,
                              expected_rows, bucket, retry_time)
        self.task_manager.add_new_task(_task)
        return _task

    def query_view(self, server, design_doc_name, view_name, query,
                   expected_rows=None, bucket="default", retry_time=2,
                   timeout=None):
        """
        Synchronously query a views in a design doc

        Parameters:
          server - The server to handle query view task. (TestInputServer)
          design_doc_name - Design doc with view(s) being queried(String)
          view_name - The view being queried (String)
          expected_rows - The number of rows expected to be returned from
                          the query (int)
          bucket - Name of the bucket containing items for this view (String)
          retry_time - Time in seconds to wait before retrying
                       failed queries (int)

        Returns:
          ViewQueryTask - Task future that is a handle to the scheduled task
        """
        _task = self.async_query_view(server, design_doc_name, view_name,
                                      query, expected_rows, bucket, retry_time)
        return self.task_manager.get_task_result(_task)

    def perform_verify_queries(self, server, num_views, prefix, ddoc_name,
                               view_name, query, expected_rows,
                               wait_time=120, bucket="default",
                               retry_time=2):
        tasks = []
        result = True
        for i in range(num_views):
            tasks.append(self.async_query_view(
                server, prefix + ddoc_name, view_name + str(i), query,
                expected_rows, bucket, retry_time))
        try:
            for task in tasks:
                self.task_manager.get_task_result(task)
                if not task.result:
                    self.log.error("Task '%s' failed" % task.thread_name)
                result = result and task.result
        except Exception as e:
            self.log.error(e)
            for task in tasks:
                task.cancel()
            raise Exception(
                "View query failed to get expected results after %s secs"
                % wait_time)
        return result

    @staticmethod
    def make_default_views(default_view, prefix, count,
                           is_dev_ddoc=False, different_map=False):
        ref_view = default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        if different_map:
            views = []
            for i in range(count):
                views.append(View(ref_view.name + str(i),
                                  'function (doc, meta) {'
                                  'emit(meta.id, "emitted_value%s");}'
                                  % str(i), None, is_dev_ddoc))
            return views
        else:
            return [
                View("{0}{1}".format(ref_view.name, i),
                     ref_view.map_func, None, is_dev_ddoc)
                for i in range(count)
            ]

    @staticmethod
    def base_bucket_ratio(servers):
        # check if ip is same for all servers
        ip = servers[0].ip
        dev_environment = True
        for server in servers:
            if server.ip != ip:
                dev_environment = False
                break
        if dev_environment:
            ratio = 2.0 / 3.0 * 1 / len(servers)
        else:
            ratio = 2.0 / 3.0
        return ratio

    def set_flusher_total_batch_limit(self, cluster, buckets,
                                      flusher_total_batch_limit=3):
        self.log.debug("Changing the bucket properties by changing "
                       "flusher_total_batch_limit to {0}"
                       .format(flusher_total_batch_limit))
        rest = RestConnection(cluster.master)

        # Enable diag_eval outside localhost
        shell = RemoteMachineShellConnection(cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        for bucket in buckets:
            code = "ns_bucket:update_bucket_props(\"" + bucket.name \
                   + "\", [{extra_config_string, " \
                   + "\"flusher_total_batch_limit=" \
                   + str(flusher_total_batch_limit) + "\"}])."
            rest.diag_eval(code)

        kv_nodes = self.cluster_util.get_kv_nodes(cluster)
        # Restart Memcached in all cluster nodes to reflect the settings
        for server in kv_nodes:
            shell = RemoteMachineShellConnection(server)
            shell.kill_memcached()
            shell.disconnect()

        # Check bucket-warm_up after Memcached restart
        retry_count = 10
        buckets_warmed_up = self.is_warmup_complete(buckets,
                                                    retry_count)
        if not buckets_warmed_up:
            self.log.critical("Few bucket(s) not warmed up "
                              "within expected time")

        for server in kv_nodes:
            for bucket in buckets:
                mc = MemcachedClientHelper.direct_client(server, bucket)
                stats = mc.stats()
                mc.close()
                self.assertTrue(int(stats['ep_flusher_total_batch_limit'])
                                == flusher_total_batch_limit)

    def update_bucket_props(self, command, value, cluster,
                            buckets=[], node=None):
        self.log.info("Changing the bucket properties by changing {0} to {1}".
                      format(command, value))
        if not buckets:
            buckets = cluster.buckets
        if node is None:
            node = cluster.master
        rest = RestConnection(node)

        shell = RemoteMachineShellConnection(node)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        for bucket in buckets:
            cmd = 'ns_bucket:update_bucket_props(' \
                  '"%s", [{extra_config_string, "%s=%s"}]).' \
                  % (bucket.name, command, value)
            rest.diag_eval(cmd)

        # Restart Memcached in all cluster nodes to reflect the settings
        for server in self.cluster_util.get_kv_nodes(cluster):
            shell = RemoteMachineShellConnection(server)
            shell.restart_couchbase()
            shell.disconnect()

        # Check bucket-warm_up after Couchbase restart
        retry_count = 10
        buckets_warmed_up = self.is_warmup_complete(buckets,
                                                    retry_count)
        if not buckets_warmed_up:
            self.log.critical("Few bucket(s) not warmed up "
                              "within expected time")

    def cbepctl_set_metadata_purge_interval(self, cluster, buckets, value):
        self.log.info("Changing the bucket properties by changing {0} to {1}".
                      format("persistent_metadata_purge_age", value))
        for node in self.cluster_util.get_kv_nodes(cluster):
            shell_conn = RemoteMachineShellConnection(node)
            cbepctl_obj = Cbepctl(shell_conn)
            for bucket in buckets:
                cbepctl_obj.set(bucket.name,
                                "flush_param",
                                "persistent_metadata_purge_age",
                                value)
            shell_conn.disconnect()

    def set_metadata_purge_interval(self, cluster, value, buckets, node):
        self.log.info("Changing the bucket properties by changing {0} to {1}".
                      format("purge_interval", value))
        rest = RestConnection(node)
        shell = RemoteMachineShellConnection(node)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        for bucket in buckets:
            cmd = '{ok, BC} = ns_bucket:get_bucket("%s"), BC2 = lists:keyreplace(purge_interval, 1, BC, {purge_interval, %s}), ns_bucket:set_bucket_config("%s", BC2).' % (
                bucket.name, value, bucket.name)
            rest.diag_eval(cmd)

        # Restart Memcached in all cluster nodes to reflect the settings
        for server in self.cluster_util.get_kv_nodes(cluster):
            shell = RemoteMachineShellConnection(server)
            shell.restart_couchbase()
            shell.disconnect()

        # Check bucket-warm_up after Couchbase restart
        retry_count = 10
        buckets_warmed_up = self.is_warmup_complete(buckets,
                                                    retry_count)
        if not buckets_warmed_up:
            self.log.critical("Few bucket(s) not warmed up "
                              "within expected time")

    @staticmethod
    def validate_manifest_uid(cluster_node, bucket):
        status = True
        manifest_uid = BucketHelper(cluster_node) \
            .get_bucket_manifest_uid(bucket)
        if bucket.stats.manifest_uid != int(manifest_uid, 16):
            BucketUtils.log.debug("Bucket UID mismatch. "
                                  "Expected: %s, Actual: %s"
                                  % (bucket.stats.manifest_uid,
                                     manifest_uid))
            status = False
        return status

    @staticmethod
    def get_expected_total_num_items(bucket):
        """
        Function to calculate the expected num_items under the given bucket.
        This will aggregate all active collections' num_items under
        each active scopes for the given bucket object
        """
        num_items = 0
        for scope in ScopeUtils.get_active_scopes(bucket):
            for collection in CollectionUtils.get_active_collections(
                    bucket,
                    scope.name):
                num_items += collection.num_items
        return num_items

    def validate_bucket_collection_hierarchy(self, bucket,
                                             cbstat_obj_list,
                                             collection_data):
        """
        API to validate the Bucket-Scope-Collection hierarchy is maintained
        as required by the given test.
        Uses bucket_obj.scope dict to validate against cbstats output.

        :param bucket: Bucket object for hierarchy reference
        :param cbstat_obj_list: List of Cbstats object to fetch required stats
        :param collection_data: Dict representing the aggregated
                                collection's num_items as per cbstats

        :return status: Boolean value representing the validation status.
                        'True' for success, 'False' for failure
        """
        status = True
        scope_data = cbstat_obj_list[0].get_scopes(bucket)
        active_scopes = ScopeUtils.get_active_scopes(bucket)
        # Validate scope count
        if len(active_scopes) != scope_data["count"]:
            status = False
            self.log.error("Mismatch in scope count in cbstats. "
                           "Expected: %s, Actual: %s"
                           % (len(active_scopes), scope_data["count"]))

        for scope in BucketUtils.get_active_scopes(bucket):
            active_collections = \
                CollectionUtils.get_active_collections(bucket, scope.name)
            # Validate collection count under current scope
            if len(active_collections) \
                    != scope_data[scope.name]["collections"]:
                status = False
                self.log.error("Mismatch in collection count for scope '%s' "
                               "in cbstats. Expected: %s, Actual: %s"
                               % (scope.name,
                                  len(active_collections),
                                  scope_data[scope.name]["collections"]))

            # Validate expected collection values
            for collection in active_collections:
                expected_items = collection_data[scope.name][
                    collection.name]["items"]
                if expected_items != collection.num_items:
                    status = False
                    self.log.error(
                        "Mismatch in %s::%s doc_count. "
                        "Expected: %s, Actual: %s"
                        % (scope.name, collection.name,
                           expected_items,
                           collection.num_items))
        return status

    def get_doc_count_per_collection(self, cluster, bucket):
        """
        Fetch total items per collection.

        :param cluster: Target cluster object
        :param bucket: Bucket object using which the validation should be done
        """
        collection_data = None
        for node in self.cluster_util.get_kv_nodes(cluster):
            cb_stat = Cbstats(node)
            tem_collection_data = cb_stat.get_collections(bucket)
            cb_stat.disconnect()
            if collection_data is None:
                collection_data = tem_collection_data
            else:
                for key, value in tem_collection_data.items():
                    if type(value) is dict:
                        for col_name, c_data in value.items():
                            collection_data[key][col_name]['items'] \
                                += c_data['items']
        return collection_data

    def validate_doc_count_as_per_collections(self, cluster, bucket):
        """
        Function to validate doc_item_count as per the collection object's
        num_items value against cbstats count from KV nodes

        Throws exception if mismatch in stats.

        :param cluster: Target cluster object
        :param bucket: Bucket object using which the validation should be done
        """
        cb_stat_objects = list()
        collection_data = self.get_doc_count_per_collection(cluster, bucket)

        # Create required cb_stat objects
        for node in self.cluster_util.get_kv_nodes(cluster):
            cb_stat_objects.append(Cbstats(node))

        # Validate scope-collection hierarchy with doc_count
        status = self.validate_bucket_collection_hierarchy(bucket,
                                                           cb_stat_objects,
                                                           collection_data)
        for cb_stat in cb_stat_objects:
            cb_stat.disconnect()
        # Raise exception if the status is 'False'
        if not status:
            raise Exception("Collections stat validation failed")

    def validate_docs_per_collections_all_buckets(self, cluster, timeout=1200,
                                                  num_zone=1):
        if cluster.type == "default":
            for bucket in cluster.buckets:
                self.get_updated_bucket_server_list(cluster, bucket)
        self.log.info("Validating collection stats and item counts")
        vbucket_stats = self.get_vbucket_seqnos(
            self.cluster_util.get_kv_nodes(cluster),
            cluster.buckets,
            skip_consistency=True)

        # Validate total expected doc_count matches with the overall bucket
        for bucket in cluster.buckets:
            expected_num_items = self.get_expected_total_num_items(bucket)
            self.verify_stats_for_bucket(cluster, bucket, expected_num_items,
                                         timeout=timeout, num_zone=num_zone)

            status = self.validate_manifest_uid(cluster.master, bucket)
            if not status:
                self.log.debug("Bucket manifest UID mismatch!")

            result = self.validate_seq_no_stats(vbucket_stats[bucket.name])
            self.assertTrue(result,
                            "snap_start and snap_end corruption found!!!")

            self.validate_doc_count_as_per_collections(cluster, bucket)

    def validate_serverless_buckets(self, cluster, buckets):
        def get_server_bucket(target_bucket):
            for s_bucket in server_buckets:
                if s_bucket["name"] == target_bucket.name:
                    return s_bucket

        def get_server_node(server_key):
            ip, port = server_key.split(":")
            for t_node in server_nodes:
                if t_node.ip == ip and int(t_node.port) == int(port):
                    return t_node

        result = True
        rest_endpoint_node = cluster.master
        if cluster.type == "serverless":
            rest_endpoint_node = buckets[0].servers[0]

        rest = RestConnection(rest_endpoint_node)
        helper = BucketHelper(rest_endpoint_node)
        # Stats wrt Cluster nodes
        server_buckets = helper.get_buckets_json()
        server_nodes = rest.get_nodes()
        # Stats wrt the test
        test_values = dict()

        kv_limit_stat_fields = ["buckets", "weight", "memory"]
        nodes_utilization_values = dict()
        for field in kv_limit_stat_fields:
            nodes_utilization_values[field] = 0

        for t_bucket in buckets:
            self.log.info("Validating serverless bucket %s" % t_bucket.name)
            # Validate bucket.width is matching as expected
            if cluster.type == "serverless":
                rest_endpoint = t_bucket.serverless.nebula_endpoint
                helper = BucketHelper(rest_endpoint)
                server_buckets = helper.get_buckets_json()
            width = t_bucket.serverless.width
            weight = t_bucket.serverless.weight
            server_bucket = get_server_bucket(t_bucket)
            req_len = width * CbServer.Serverless.KV_SubCluster_Size
            server_len = len(server_bucket["nodes"])
            self.log.debug("%s - required server_list_len=%s, actual=%s"
                           % (t_bucket.name, req_len, server_len))
            if req_len != server_len:
                result = False
                self.log.critical("Bucket %s, width=%s expected to be "
                                  "placed on %s nodes, but occupies %s nodes"
                                  % (t_bucket.name, width, req_len,
                                     len(server_bucket["nodes"])))

            # Validate bucket.num_vbs against expected value
            num_vb = t_bucket.numVBuckets or CbServer.Serverless.VB_COUNT
            self.log.debug("%s - required vb_num=%s, actual=%s"
                           % (t_bucket.name,
                              server_bucket[Bucket.numVBuckets], num_vb))
            if num_vb != server_bucket[Bucket.numVBuckets]:
                result = False
                self.log.critical("Expected num_vbuckets=%s, actual %s"
                                  % (num_vb,
                                     server_bucket[Bucket.numVBuckets]))

            # Construct AZ map to validate the bucket distribution
            # across the server groups
            az_map = dict()
            for node in server_bucket["nodes"]:
                if node["serverGroup"] not in az_map:
                    az_map[node["serverGroup"]] = 0
                az_map[node["serverGroup"]] += 1
            tem_width = list(set(az_map.values()))
            self.log.debug("%s - Expected width=%s, actual=%s"
                           % (t_bucket.name, width, tem_width[0]))
            if len(tem_width) != 1:
                result = False
                self.log.critical("Uneven bucket distribution: %s" % az_map)
            elif tem_width[0] != width:
                result = False
                self.log.critical("Expected bucket width %s, actual %s"
                                  % (width, tem_width[0]))

            # Calculate servers' utilization values as per the test
            for b_server in t_bucket.servers:
                key = "%s:%s" % (b_server.ip, b_server.port)
                if key not in test_values:
                    # Init values for new server
                    test_values[key] = dict()
                    for field in kv_limit_stat_fields:
                        test_values[key][field] = 0

                test_values[key]["buckets"] += 1
                test_values[key]["weight"] += weight
                test_values[key]["memory"] += t_bucket.ramQuotaMB * 1048576

        # Begin limit/utilization validation
        for node_ip, test_stats in test_values.items():
            server_node = get_server_node(node_ip)
            server_kv_limits = server_node.limits[CbServer.Services.KV]
            server_kv_usage = server_node.utilization[CbServer.Services.KV]
            # MAX_WEIGHT check
            self.log.debug("%s - max_weight=%s" % (node_ip,
                                                   server_kv_limits["weight"]))
            if server_kv_limits["weight"] != CbServer.Serverless.MAX_WEIGHT:
                result = False
                self.log.critical("%s - Expected limits::weight=%s, actual=%s"
                                  % (node_ip, CbServer.Serverless.MAX_WEIGHT,
                                     server_kv_limits["weight"]))
            # MAX_BUCKETS check
            self.log.debug("%s - max_buckets=%s"
                           % (node_ip, server_kv_limits["buckets"]))
            if server_kv_limits["buckets"] != CbServer.Serverless.MAX_BUCKETS:
                result = False
                self.log.critical("%s - Expected limits::buckets=%s, actual=%s"
                                  % (node_ip, CbServer.Serverless.MAX_BUCKETS,
                                     server_kv_limits["buckets"]))
            # MEMORY usage check
            self.log.debug("%s - Mem max=%s, Usage expected=%s, actual=%s"
                           % (node_ip, server_kv_limits["memory"],
                              test_stats["memory"],server_kv_usage["memory"]))
            if server_kv_usage["memory"] > server_kv_limits["memory"]:
                result = False
                self.log.critical("%s - memory usage more than "
                                  "expected=%s < actual=%s"
                                  % (node_ip, server_kv_limits["memory"],
                                     server_kv_usage["memory"]))
            if server_kv_usage["memory"] != test_stats["memory"]:
                result = False
                self.log.critical("%s - Memory usage mismatch. "
                                  "Expected=%s, actual=%s"
                                  % (node_ip, test_stats["memory"],
                                     server_kv_usage["memory"]))
            # BUCKETS check
            self.log.debug("%s - Num_Buckets expected=%s, actual=%s"
                           % (node_ip, test_stats["buckets"],
                              server_kv_usage["buckets"]))
            if server_kv_usage["buckets"] != test_stats["buckets"]:
                result = False
                self.log.critical("%s - Mismatch in num_buckets. "
                                  "Expected=%s, actual=%s"
                                  % (node_ip, test_stats["buckets"],
                                     server_kv_usage["buckets"]))
            # WEIGHT check
            self.log.debug("%s - Node_weight expected=%s, actual=%s"
                           % (node_ip, test_stats["weight"],
                              server_kv_usage["weight"]))
            if server_kv_usage["weight"] != test_stats["weight"]:
                result = False
                self.log.critical("%s - Mismatch in weight. "
                                  "Expected=%s, actual=%s"
                                  % (node_ip, test_stats["weight"],
                                     server_kv_usage["weight"]))
        return result

    def remove_scope_collections_for_bucket(self, cluster, bucket):
        """
        Delete all created scope-collection for the given bucket
        """
        for scope in self.get_active_scopes(bucket):
            # we can't remove the _system scope
            if scope.name == CbServer.system_scope:
                continue

            for collection in self.get_active_collections(bucket, scope.name):
                self.drop_collection(cluster.master,
                                     bucket,
                                     scope_name=scope.name,
                                     collection_name=collection.name)

            # _default scope can never be dropped
            if scope.name != CbServer.default_scope:
                self.drop_scope(cluster.master,
                                bucket,
                                scope_name=scope.name)

    @staticmethod
    def perform_tasks_from_spec(cluster, buckets, input_spec):
        """
        Perform Create/Drop/Flush of scopes and collections as specified
        in the input json spec template.
        Scopes/collections drop/creation is done using threads instead
        of sequential manner

        :param cluster: Cluster object to fetch master node
                        (To create REST connections for bucket operations)
        :param buckets: List of bucket objects considered for bucket ops
        :param input_spec: CRUD spec (JSON format)
        :return ops_details: Dict describing the actions taken during the
                             execution using input_spec
        """

        def update_ops_details_dict(target_key, bucket_data_to_update,
                                    fetch_collections=False):
            dict_to_update = ops_details[target_key]
            for bucket_name, b_data in bucket_data_to_update.items():
                if bucket_name not in dict_to_update:
                    dict_to_update[bucket_name] = dict()
                    dict_to_update[bucket_name]["scopes"] = dict()
                scope_dict = dict_to_update[bucket_name]["scopes"]
                for scope_name, scope_data in b_data["scopes"].items():
                    if scope_name not in scope_dict:
                        scope_dict[scope_name] = dict()
                        scope_dict[scope_name]["collections"] = dict()
                    collection_dict = scope_dict[scope_name]["collections"]
                    if fetch_collections:
                        bucket = BucketUtils.get_bucket_obj(buckets,
                                                            bucket_name)
                        scope_data["collections"] = \
                            bucket.scopes[scope_name].collections

                    for collection_name in list(scope_data["collections"].keys()):
                        collection_dict[collection_name] = dict()

        def perform_scope_operation(operation_type, ops_spec):
            buckets_spec = ops_spec
            if "buckets" in buckets_spec:
                buckets_spec = ops_spec["buckets"]
            scope_counter = Bucket.scope_counter
            col_counter = Bucket.collection_counter
            if operation_type == "create":
                scopes_num = ops_spec["scopes_num"]
                collection_count = \
                    ops_spec["collection_count_under_each_scope"]
                for bucket_name in buckets_spec:
                    bucket = BucketUtils.get_bucket_obj(buckets,
                                                        bucket_name)
                    if bucket_name not in ops_details["scopes_added"]:
                        ops_details["scopes_added"][bucket_name] = dict()
                        ops_details["scopes_added"][bucket_name][
                            "scopes"] = dict()
                    if bucket_name not in ops_details["collections_added"]:
                        ops_details["collections_added"][bucket_name] = dict()
                        ops_details["collections_added"][bucket_name][
                            "scopes"] = dict()
                    # {scope_name:{"collections":{collection_name:{}}}}
                    created_scope_collection_details = dict()
                    for _ in range(scopes_num):
                        scope_name = BucketUtils.get_random_name(
                            prefix='scope',
                            counter_obj=scope_counter)
                        ScopeUtils.create_scope(cluster.master,
                                                bucket,
                                                {"name": scope_name})
                        created_scope_collection_details[scope_name] = dict()
                        created_scope_collection_details[scope_name]["collections"] = dict()
                        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = dict()
                            with requests.Session() as session:
                                for _ in range(collection_count):
                                    collection_name = \
                                        BucketUtils.get_random_name(
                                            prefix='collection',
                                            counter_obj=col_counter)
                                    futures[executor.submit(
                                        BucketUtils.create_collection,
                                        cluster.master, bucket, scope_name,
                                        {"name": collection_name},
                                        session=session)] = collection_name
                            for future in concurrent.futures.as_completed(futures):
                                if future.exception() is not None:
                                    raise future.exception()
                                else:
                                    created_scope_collection_details[scope_name]["collections"][
                                        futures[future]] = dict()
                    ops_details["scopes_added"][bucket_name][
                        "scopes"].update(created_scope_collection_details)
                    ops_details["collections_added"][bucket_name][
                        "scopes"].update(created_scope_collection_details)
            elif operation_type == "drop":
                update_ops_details_dict("scopes_dropped", buckets_spec,
                                        fetch_collections=True)
                update_ops_details_dict("collections_dropped", buckets_spec,
                                        fetch_collections=True)
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    with requests.Session() as session:
                        for bucket_name, b_data in buckets_spec.items():
                            bucket = BucketUtils.get_bucket_obj(buckets,
                                                                bucket_name)
                            for scope_name in list(b_data["scopes"].keys()):
                                futures.append(executor.submit(ScopeUtils.drop_scope, cluster.master,
                                                               bucket, scope_name, session=session))
                    for future in concurrent.futures.as_completed(futures):
                        if future.exception() is not None:
                            raise future.exception()
            elif operation_type == "recreate":
                DocLoaderUtils.log.debug("Recreating scopes")
                update_ops_details_dict("scopes_added", buckets_spec)
                update_ops_details_dict("collections_added", buckets_spec,
                                        fetch_collections=True)
                for bucket_name, b_data in buckets_spec.items():
                    bucket = BucketUtils.get_bucket_obj(buckets,
                                                        bucket_name)
                    for scope_name in list(b_data["scopes"].keys()):
                        scope_obj = bucket.scopes[scope_name]
                        BucketUtils.create_scope(cluster.master,
                                                 bucket,
                                                 scope_obj.get_dict_object())
                        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = []
                            with requests.Session() as session:
                                for _, collection in scope_obj.collections.items():
                                    c_dict = collection.get_dict_object()
                                    if "nonDedupedHistory" not in bucket.bucketCapabilities:
                                        c_dict.pop("history", None)
                                    futures.append(executor.submit(ScopeUtils.create_collection, cluster.master,
                                                                   bucket, scope_name, c_dict,
                                                                   session))
                            for future in concurrent.futures.as_completed(futures):
                                if future.exception() is not None:
                                    raise future.exception()

        def perform_collection_operation(operation_type, ops_spec):
            if operation_type == "create":
                bucket_names = list(ops_spec.keys())
                bucket_names.remove("req_collections")
                net_dict = dict()  # {"buckets"{bucket_name:{"scopes":{scope_name:{"collections":{collections_name:{}}}}}}}
                net_dict["buckets"] = dict()
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = dict()
                    with requests.Session() as session:
                        for _ in range(ops_spec["req_collections"]):
                            bucket_name = sample(bucket_names, 1)[0]
                            bucket = BucketUtils.get_bucket_obj(buckets,
                                                                bucket_name)
                            scope = sample(
                                list(ops_spec[bucket_name]["scopes"].keys()),
                                1)[0]
                            collection_name = BucketUtils.get_random_name(
                                prefix="collection",
                                counter_obj=Bucket.collection_counter)
                            if bucket_name not in net_dict["buckets"]:
                                net_dict["buckets"][bucket_name] = dict()
                                net_dict["buckets"][bucket_name]["scopes"] = dict()
                            if scope not in net_dict["buckets"][bucket_name]["scopes"]:
                                net_dict["buckets"][bucket_name]["scopes"][scope] = dict()
                                net_dict["buckets"][bucket_name]["scopes"][scope]["collections"] = dict()
                            net_dict["buckets"][bucket_name]["scopes"][scope]["collections"][collection_name] = dict()
                            futures[executor.submit(BucketUtils.create_collection, cluster.master,
                                                    bucket, scope, {"name": collection_name},
                                                    session=session)] = bucket_name
                    for future in concurrent.futures.as_completed(futures):
                        if future.exception() is not None:
                            raise future.exception()
                        else:
                            update_ops_details_dict("collections_added",
                                                    {futures[future]: net_dict["buckets"][futures[future]]})
            elif operation_type in ["flush", "drop"]:
                if operation_type == "drop":
                    update_ops_details_dict("collections_dropped", ops_spec)
                else:
                    update_ops_details_dict("collections_flushed", ops_spec)

                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    with requests.Session() as session:
                        for bucket_name, b_data in ops_spec.items():
                            bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
                            for scope_name, scope_data in b_data["scopes"].items():
                                for collection_name in \
                                        list(scope_data["collections"].keys()):
                                    if operation_type == "flush":
                                        CollectionUtils.flush_collection(
                                            cluster.master, bucket,
                                            scope_name, collection_name)
                                    elif operation_type == "drop":
                                        futures.append(executor.submit(CollectionUtils.drop_collection, cluster.master,
                                                                       bucket, scope_name, collection_name,
                                                                       session=session))
                    for future in concurrent.futures.as_completed(futures):
                        if future.exception() is not None:
                            raise future.exception()
            elif operation_type == "recreate":
                DocLoaderUtils.log.debug("Recreating collections")
                update_ops_details_dict("collections_added", ops_spec)
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    with requests.Session() as session:
                        for bucket_name, b_data in ops_spec.items():
                            bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
                            for scope_name, scope_data in b_data["scopes"].items():
                                for col_name in list(scope_data["collections"].keys()):
                                    collection = bucket.scopes[scope_name] \
                                        .collections[col_name]
                                    c_dict = collection.get_dict_object()
                                    if "nonDedupedHistory" not in bucket.bucketCapabilities:
                                        c_dict.pop("history", None)
                                    rest = RestConnection(cluster.master)
                                    clusterCompat = rest.check_cluster_compatibility("7.6")
                                    if not clusterCompat:
                                        c_dict.pop("maxTTL", None)
                                    futures.append(executor.submit(ScopeUtils.create_collection, cluster.master,
                                                                   bucket, scope_name, c_dict,
                                                                   session=session))
                    for future in concurrent.futures.as_completed(futures):
                        if future.exception() is not None:
                            raise future.exception()

        DocLoaderUtils.log.info("Performing scope/collection specific "
                                "operations")

        max_workers = input_spec.get(MetaCrudParams.THREADPOOL_MAX_WORKERS, 10)

        # To store the actions wrt bucket/scope/collection
        ops_details = dict()
        ops_details["scopes_added"] = dict()
        ops_details["scopes_dropped"] = dict()
        ops_details["collections_added"] = dict()
        ops_details["collections_dropped"] = dict()
        ops_details["collections_flushed"] = dict()

        # Fetch random Collections to flush
        cols_to_flush = dict()
        # cols_to_flush = BucketUtils.get_random_collections(
        #     buckets,
        #     ops_spec[MetaCrudParams.COLLECTIONS_TO_FLUSH],
        #     ops_spec[MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS],
        #     ops_spec[MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS])

        # Fetch random Collections to drop
        exclude_from = copy.deepcopy(cols_to_flush)
        # Code to avoid removing collections under the scope '_system'
        dict_to_update = dict()
        for b in buckets:
            dict_to_update[b.name] = dict()
            dict_to_update[b.name]["scopes"] = dict()
            dict_to_update[b.name]["scopes"][CbServer.system_scope] = {
                "collections": {
                    CbServer.eventing_collection: {},
                    CbServer.query_collection: {},
                    CbServer.mobile_collection: {},
                    CbServer.regulator_collection: {},
                }

            }
            for scope in b.scopes:
                for collection in b.scopes[scope].collections:
                    if 'skip_dict' in input_spec and b.name in input_spec[
                        'skip_dict'] and 'scopes' in input_spec[
                        'skip_dict'][b.name] and scope in input_spec[
                        'skip_dict'][b.name]['scopes'] and 'collections' in \
                            input_spec['skip_dict'][b.name]['scopes'][scope] \
                            and collection in input_spec['skip_dict'][b.name][
                                               'scopes'][scope]['collections']:
                        if 'scopes' not in dict_to_update[b.name]:
                            dict_to_update[b.name]['scopes'] = dict()
                        if scope not in dict_to_update[b.name]['scopes']:
                            dict_to_update[b.name]['scopes'][scope] = dict()
                        if 'collections' not in dict_to_update[b.name][
                            'scopes'][scope]:
                            dict_to_update[b.name]['scopes'][scope]['collections'] = dict()
                        if collection not in dict_to_update[b.name][
                            'scopes'][scope]['collections']:
                            dict_to_update[b.name]['scopes'][scope]['collections'][
                                collection] = dict()
            exclude_from.update(dict_to_update)
        # If recreating dropped collections then exclude dropping default collection
        if input_spec.get(MetaCrudParams.COLLECTIONS_TO_RECREATE, 0):
            for bucket in buckets:
                if bucket.name not in exclude_from:
                    exclude_from[bucket.name] = dict()
                    exclude_from[bucket.name]["scopes"] = dict()
                if CbServer.default_scope not in exclude_from[bucket.name]["scopes"]:
                    exclude_from[bucket.name]["scopes"][CbServer.default_scope] = dict()
                    exclude_from[bucket.name]["scopes"][CbServer.default_scope]["collections"] = dict()
                exclude_from[bucket.name]["scopes"][CbServer.default_scope]["collections"][
                    CbServer.default_collection] = dict()
        cols_to_drop = BucketUtils.get_random_collections(
            buckets,
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_DROP, 0),
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, 1),
            exclude_from=exclude_from)

        # Drop or flush collections
        perform_collection_operation("flush", cols_to_flush)
        perform_collection_operation("drop", cols_to_drop)

        # Fetch scopes to be dropped, and drop them
        scopes_to_drop = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_TO_DROP, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, 1),
            avoid_default=True)
        perform_scope_operation("drop", scopes_to_drop)

        # Fetch buckets under which scopes will be created, and create them
        create_scope_spec = dict()
        create_scope_spec["buckets"] = BucketUtils.get_random_buckets(
            buckets,
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"))
        create_scope_spec["scopes_num"] = \
            input_spec.get(MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET, 0)
        create_scope_spec["collection_count_under_each_scope"] = \
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES, 0)
        perform_scope_operation("create", create_scope_spec)

        # Fetch random Scopes under which to create collections, and create them
        scopes_to_create_collections = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, "all"),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"))
        scopes_to_create_collections["req_collections"] = \
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET, 0)
        perform_collection_operation("create", scopes_to_create_collections)

        # Fetch random scopes to recreate, and recreate them
        scopes_to_recreate = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_TO_RECREATE, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"),
            consider_only_dropped=True)
        perform_scope_operation("recreate", scopes_to_recreate)

        # Fetch random collections to recreate, and recreate them
        collections_to_recreate = BucketUtils.get_random_collections(
            buckets,
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_RECREATE, 0),
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, "all"),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"),
            consider_only_dropped=True, exclude_from=exclude_from)
        perform_collection_operation("recreate", collections_to_recreate)
        DocLoaderUtils.log.info("Done Performing scope/collection specific "
                                "operations")
        return ops_details

    @staticmethod
    def get_total_collections_in_bucket(bucket):
        collection_count = 0
        active_scopes = BucketUtils.get_active_scopes(bucket)
        for scope in active_scopes:
            if scope.is_dropped:
                continue
            collection_count += len(BucketUtils.get_active_collections(
                bucket, scope.name, only_names=True))
        return collection_count

    @staticmethod
    def get_total_items_count_in_a_collection(cluster, bucket_name,
                                              scope_name, collection_name):
        """
        Returns (int): Total item count in a given collection using range api
        """
        end_time = int(round(time.time()))
        start_time = end_time - 10
        metric_name = "kv_collection_item_count"
        label_values = {"bucket": bucket_name,
                        "scope": scope_name,
                        "collection": collection_name,
                        "nodesAggregation": "sum",
                        "start": start_time, "end": end_time}
        _, content = ClusterRestAPI(cluster.master). \
            get_stats_for_metric(metric_name, label_values=label_values)
        item_count = content["data"][0]["values"][-1][-1]
        return int(item_count)

    def get_stat_from_metrics(self, bucket):
        num_throttled, ru, wu = 0, 0, 0
        for server in bucket.servers:
            _, output = RestConnection(server).get_prometheus_metrics()
            output = output.splitlines()
            if "Forbidden" in output:
                self.log.error("Prometheus stats failed with error %s" % output)
            wu_pattern = re.compile('meter_wu_total{bucket="%s",for="kv"} (\d+)' % bucket.name)
            ru_pattern = re.compile('meter_ru_total{bucket="%s",for="kv"} (\d+)' % bucket.name)
            num_throttled_pattern = re.compile('throttle_count_total{bucket="%s",for="kv"} (\d+)' % bucket.name)
            for line in output:
                if wu_pattern.match(line):
                    wu += int(wu_pattern.findall(line)[0])
                    break
            for line in output:
                if ru_pattern.match(line):
                    ru += int(ru_pattern.findall(line)[0])
                    break
            for line in output:
                if num_throttled_pattern.match(line):
                    num_throttled += int(num_throttled_pattern.findall(line)[0])
                    break
        self.log.info("num_throttled %s, ru %s, wu %s" % (num_throttled, ru, wu))
        return num_throttled, ru, wu

    def set_throttle_n_storage_limit(self, bucket, throttle_limit=5000, storage_limit=500, service="data"):
        for server in bucket.servers:
            bucket_helper = BucketHelper(server)
            _, content = bucket_helper.set_throttle_n_storage_limit(bucket_name=bucket.name,
                                                          throttle_limit=throttle_limit,
                                                          storage_limit=storage_limit,
                                                          service=service)

    def get_throttle_limit(self, bucket):
        bucket_helper = BucketHelper(bucket.servers[0])
        output = bucket_helper.get_buckets_json(bucket.name)
        return output["dataThrottleLimit"]

    def get_storage_limit(self, bucket):
        bucket_helper = BucketHelper(bucket.servers[0])
        output = bucket_helper.get_buckets_json(bucket.name)
        return output["dataStorageLimit"]

    def get_disk_used(self, bucket):
        bucket_helper = BucketHelper(bucket.servers[0])
        output = bucket_helper.get_buckets_json(bucket.name)
        return output["basicStats"]["diskUsed"]

    def get_items_count(self, bucket):
        bucket_helper = BucketHelper(bucket.servers[0])
        output = bucket_helper.get_buckets_json(bucket.name)
        return output["basicStats"]["itemCount"]

    def get_storage_quota(self, bucket):
        output = RestConnection(bucket.servers[0]).get_pools_default()
        storage_quota = output["storageTotals"]["hdd"]["quotaTotal"]
        return storage_quota

    def calculate_units(self, key, value, sub_doc_size=0, xattr=0,
                        read=False, num_items=1, durability="NONE"):
        if read:
            limit = 4096
        else:
            limit = 1024
        total_size = key + value + sub_doc_size + xattr
        expected_cu, remainder = divmod(total_size, limit)
        if remainder:
            expected_cu += 1
        if durability != "NONE" and not read:
            expected_cu *= 2
        return expected_cu * num_items

    def get_total_items_bucket(self, bucket):
        sum = 0
        bucket_helper = BucketHelper(bucket.servers[0])
        for server in bucket.servers:
            server_stats = bucket_helper.get_bucket_stats_for_node(
                bucket, server)
            try:
                sum += server_stats["curr_items"]
            except:
                self.log.info("bucket is not loaded")
                break
        return sum

    def get_initial_stats(self, buckets):
        expected_stats = dict()
        for bucket in buckets:
            self.get_throttle_limit(bucket)
            expected_stats[bucket.name] = dict()
            expected_stats[bucket.name]["num_throttled"], \
                expected_stats[bucket.name]["ru"], \
                expected_stats[bucket.name]["wu"] = \
                self.get_stat_from_metrics(bucket)
            expected_stats[bucket.name]["total_items"] = 0
        return expected_stats

    def update_stat_on_buckets(self, buckets, expected_stats,
                               write_units, read_units=0):
        for bucket in buckets:
            num_throttle = 0
            expected_stats[bucket.name]["wu"] += write_units
            expected_stats[bucket.name]["ru"] += read_units
            throttle_limit = self.get_throttle_limit(bucket)
            if throttle_limit != -1:
                if write_units:
                    num_throttle += write_units/self.get_throttle_limit(bucket)
                if read_units:
                    num_throttle += read_units/self.get_throttle_limit(bucket)
            expected_stats[bucket.name]["num_throttled"] += num_throttle
        return expected_stats

    def validate_stats(self, buckets, expected_stats):
        for bucket in buckets:
            num_throttle, ru, wu = self.get_stat_from_metrics(bucket)
            if num_throttle < expected_stats[bucket.name]["num_throttled"] \
                or ru < expected_stats[bucket.name]["ru"] \
                    or wu != expected_stats[bucket.name]["wu"]:
                self.log.info("actual stats %s %s %s, expected stats %s %s %s"
                              % (num_throttle, ru, wu, expected_stats[bucket.name]["num_throttled"],
                                 expected_stats[bucket.name]["ru"],
                                 expected_stats[bucket.name]["wu"]))
            else:
                self.log.info("stats matched,actual stats %s %s %s, expected stats %s %s %s"
                              % (num_throttle, ru, wu, expected_stats[bucket.name]["num_throttled"],
                                 expected_stats[bucket.name]["ru"],
                                 expected_stats[bucket.name]["wu"]))

    def get_actual_items_loaded_to_calculate_wu(self, expected_items, previous_items, bucket):
        """
        previous items - number of items bucket already had
        expected_items - number of new items we try to load
        actual items - number of items actually loaded
        """
        iteration = 0
        actual_items = expected_items
        while iteration < 4:
            actual_items = self.get_total_items_bucket(bucket) - previous_items
            if expected_items == actual_items:
                break
            sleep(5, "check the item count again")
            iteration += 1
        return actual_items

    # History retention methods
    def get_vb_details_for_bucket(self, bucket, kv_nodes):
        """
        Populates the stat_dict with the required stats for further validation
        :return stat_dict: Dict of required values
        stat_dict = {
            # vbucket-details stats
            "vb-details": {
                1: { "active": {"high_seqno": ..,
                                "history_start_seqno": ..},
                     "replica": [ {"high_seqno": ..,
                                   "history_start_seqno": ..},..]
                }
            }
        }
        """
        vb_details_fields = ["high_seqno", "history_start_seqno",
                             "purge_seqno"]
        active = Bucket.vBucket.ACTIVE
        replica = Bucket.vBucket.REPLICA
        stat_dict = dict()
        for node in kv_nodes:
            self.log.debug("Fetching vb-details for {0} from {1}"
                           .format(bucket.name, node.ip))
            cb_stat = Cbstats(node)
            vb_details = cb_stat.vbucket_details(bucket_name=bucket.name)
            if bucket.storageBackend == Bucket.StorageBackend.magma:
                max_retries = 5
                while max_retries > 0:
                    stat_ok = True
                    if bucket.storageBackend == Bucket.StorageBackend.magma:
                        for vb, _ in vb_details.items():
                            if "history_start_seqno" not in vb_details[str(vb)]:
                                stat_ok = False
                                break
                    if stat_ok:
                        break
                    max_retries -= 1
                    sleep(3, "Wait for 'hist_start_seqno' to appear in cbstats")
                    vb_details = cb_stat.vbucket_details(bucket_name=bucket.name)
                else:
                    self.log.critical("history_start_seqno not present on {}"
                                      .format(node.ip))
            else:
                vb_details_fields.remove("history_start_seqno")
                for vb, _ in vb_details.items():
                    if "history_start_seqno" in vb_details[str(vb)]:
                        self.log.critical(
                            "Hist_stat_seqno present for non-magma bucket")
            # Populate active stats
            for vb in cb_stat.vbucket_list(bucket.name, active):
                if vb not in stat_dict:
                    stat_dict[vb] = dict()
                    stat_dict[vb][active] = dict()
                    stat_dict[vb][replica] = list()
                for field in vb_details_fields:
                    stat_dict[vb][active][field] = vb_details[str(vb)][field]
            # Populate replica stats
            for vb in cb_stat.vbucket_list(bucket.name, replica):
                if vb not in stat_dict:
                    stat_dict[vb] = dict()
                    stat_dict[vb][active] = dict()
                    stat_dict[vb][replica] = list()
                replica_stat = dict()
                for field in vb_details_fields:
                    replica_stat[field] = vb_details[str(vb)][field]
                stat_dict[vb][replica].append(replica_stat)
            cb_stat.disconnect()
        return stat_dict

    def validate_history_start_seqno_stat(
            self, prev_stat, curr_stats,
            comparison="==", no_history_preserved=False):
        """
        - no_history_preserved is True, expected curr::hist_start_seqno == 0
        - comparison '==', Fail if vb_hist_start_seqno :: prev != curr
        - comparison '>', Fail if vb_hist_start_seqno :: prev > curr
        - comparison '>=', Fail if vb_hist_start_seqno :: prev >= curr
        """
        result = True
        active = Bucket.vBucket.ACTIVE
        replica = Bucket.vBucket.REPLICA
        high_seqno = "high_seqno"
        hist_start_seqno = "history_start_seqno"
        purge_seqno = "purge_seqno"
        # Check replica stats
        for index, stat in enumerate([prev_stat, curr_stats]):
            for vb_num, stats in stat.items():
                for r_stat in stats[replica]:
                    if r_stat[high_seqno] < r_stat[hist_start_seqno]:
                        result = False
                        self.log.critical(
                            "{0} - vb_{1}, replica "
                            "high_seqno {2} < {3} hist_start_seqno"
                            .format(index, vb_num, r_stat[high_seqno],
                                    r_stat[hist_start_seqno]))
                    if r_stat[hist_start_seqno] == 0:
                        result = False
                        self.log.critical(
                            "{0} - vb_{1}, replica hist_start_seqno is zero"
                            .format(index, vb_num))
                    if r_stat[purge_seqno] > r_stat[hist_start_seqno]:
                        result = False
                        self.log.critical(
                            "{0} - vb_{1}, replica "
                            "purge_seqno {2} > {3} hist_start_seqno"
                            .format(index, vb_num, r_stat[purge_seqno],
                                    r_stat[hist_start_seqno]))
        if no_history_preserved:
            for vb_num, stats in curr_stats.items():
                active_stats = stats[active]
                if active_stats[hist_start_seqno] != 0:
                    result = False
                    self.log.critical(
                        "vb_{0} history_start_seqno {1} != 0"
                        .format(vb_num, active_stats[hist_start_seqno]))
        elif comparison == "==":
            for vb_num, stats in prev_stat.items():
                active_stats = stats[active]
                if active_stats[hist_start_seqno] \
                        != curr_stats[vb_num][active][hist_start_seqno]:
                    result = False
                    self.log.critical(
                        "vb_{0} history_start_seqno mismatch. {1} != {2}"
                        .format(vb_num, active_stats[hist_start_seqno],
                                curr_stats[vb_num][active][hist_start_seqno]))
        elif comparison == ">":
            for vb_num, stats in prev_stat.items():
                prev_active_stats = stats[active]
                if prev_active_stats[hist_start_seqno] \
                        > curr_stats[vb_num][active][hist_start_seqno]:
                    result = False
                    self.log.critical(
                        "vb_{0} hist_start_seqno, prev::{1} > curr::{2}"
                        .format(vb_num, prev_active_stats[hist_start_seqno],
                                curr_stats[vb_num][active][hist_start_seqno]))

        elif comparison == ">=":
            for vb_num, stats in prev_stat.items():
                prev_active_stats = stats[active]
                if prev_active_stats[hist_start_seqno] \
                        >= curr_stats[vb_num][active][hist_start_seqno]:
                    result = False
                    self.log.critical(
                        "vb_{0} hist_start_seqno, prev::{1} >= curr::{2}"
                            .format(vb_num,
                                    prev_active_stats[hist_start_seqno],
                                    curr_stats[vb_num][active][
                                        hist_start_seqno]))
        return result

    def validate_disk_info_detail_history_stats(self, bucket, bucket_nodes,
                                                configured_size_per_vb):
        result = True
        for node in bucket_nodes:
            cbstat = Cbstats(node)
            disk_info = cbstat.disk_info_detail(bucket.name)
            for vb_num, stat in disk_info.items():
                if stat["history_disk_size"] > configured_size_per_vb:
                    result = False
                    self.log.critical(
                        "{0} - vb_{1}:hist_disk_size: {2} > {3} (configured)"
                        .format(node.ip, vb_num, stat["history_disk_size"],
                                configured_size_per_vb))
            cbstat.disconnect()
        return result

    def get_logical_data(self, bucket):
        active_logical_data = 0
        pending_logical_data = 0
        replica_logical_data = 0
        for server in bucket.servers:
            _, stats = RestConnection(server).query_prometheus("kv_logical_data_size_bytes")
            if stats["status"] == "success":
                stats = [stat for stat in stats["data"]["result"] if stat["metric"]["bucket"] == bucket.name]
                for stat in stats:
                    if stat["metric"]["state"] == "active":
                        active_logical_data += int(stat["value"][1])
                    elif stat["metric"]["state"] == "pending":
                        pending_logical_data += int(stat["value"][1])
                    elif stat["metric"]["state"] == "replica":
                        replica_logical_data += int(stat["value"][1])
        return active_logical_data, pending_logical_data, replica_logical_data

    def update_ttl_for_collections(self, cluster, bucket, ttl_value,
                                   enable_ttl=False):
        for _, scope in bucket.scopes.items():
            if scope.name == "_system":
                continue
            for _, col in scope.collections.items():
                if (col.maxTTL > 0 and not enable_ttl) \
                        or (enable_ttl and col.maxTTL == 0):
                    self.log.info("Setting maxTTL={} for {} {}"
                                  .format(ttl_value, scope.name, col.name))
                    self.set_maxTTL_for_collection(cluster.master, bucket,
                                                   scope.name, col.name,
                                                   maxttl=ttl_value)

    def disable_ttl_for_collections(self, cluster, bucket):
        for scope_name in bucket.scopes:
            for coll_name in bucket.scopes[scope_name].collections:
                col_ttl = bucket.scopes[scope_name].collections[coll_name].maxTTL
                if col_ttl > 0:
                    self.log.info("Setting ttl=0 for {0}:{1}".format(scope_name,coll_name))
                    self.set_maxTTL_for_collection(cluster.master, bucket,
                                                   scope_name, coll_name,
                                                   maxttl=0)
