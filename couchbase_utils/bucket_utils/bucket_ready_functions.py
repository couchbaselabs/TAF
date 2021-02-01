"""
Created on Sep 26, 2017

@author: riteshagarwal
"""

import copy
import importlib
import re
import threading
import datetime
from random import sample, choice

import requests
import concurrent.futures

import crc32
import exceptions
import json
import random
import string
import time
import uuid
import zlib
from subprocess import call
from collections import defaultdict

import mc_bin_client
import memcacheConstants
from BucketLib.BucketOperations import BucketHelper
from Cb_constants import CbServer, DocLoading
from Jython_tasks.task import \
    BucketCreateTask, \
    BucketCreateFromSpecTask, \
    ViewCreateTask, \
    ViewDeleteTask, \
    ViewQueryTask
from SecurityLib.rbac import RbacUtil
from TestInput import TestInputSingleton
from BucketLib.bucket import Bucket, Collection, Scope
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaConstants, \
    MetaCrudParams
from common_lib import sleep
from couchbase_helper.data_analysis_helper import DataCollector, DataAnalyzer, \
    DataAnalysisResultAnalyzer
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import doc_generator, \
    sub_doc_generator, sub_doc_generator_for_edit
from couchbase_helper.durability_helper import BucketDurability
from error_simulation.cb_error import CouchbaseError
from global_vars import logger

from membase.api.exception import StatsUnavailableException
from membase.api.rest_client import Node, RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import MemcachedClientHelper, \
    VBucketAwareMemcached
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from table_view import TableView
from testconstants import MAX_COMPACTION_THRESHOLD, \
    MIN_COMPACTION_THRESHOLD
from sdk_client3 import SDKClient
from couchbase_helper.tuq_generators import JsonGenerator

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


class DocLoaderUtils(object):
    log = logger.get("test")
    sdk_client_pool = None

    @staticmethod
    def __get_required_num_from_percentage(collection_obj, target_percent):
        """
        To calculate the num_items for mutation for given collection_obj
        :param collection_obj: Object of Bucket.scope.collection
        :param target_percent: Percentage as given in spec
        :return: num_items (int) value derived using percentage value
        """
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
                          target_vbuckets="all", type="default"):
        """
        Create doc generators based on op_type provided
        :param op_type: CRUD type
        :param collection_obj: Collection object from Bucket.scope.collection
        :param num_items: Targeted item count under the given collection
        :param generic_key: Doc_key to be used while doc generation
        :param mutation_num: Mutation number, used for doc validation task
        :param target_vbuckets: Target_vbuckets for which doc loading
                                should be done. Type: list / range()
        :return: doc_generator object based on given inputs
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

        if target_vbuckets == "all":
            target_vbuckets = None
        else:
            # Target_vbuckets doc_gen require only total items to be created
            # Hence subtracting to get only the num_items back
            end -= start

        if type == "default":
            gen_docs = doc_generator(generic_key, start, end,
                                     target_vbucket=target_vbuckets,
                                     mutation_type=op_type,
                                     mutate=mutation_num)
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
                             generic_key, target_vbuckets="all", xattr_test=False):
        if target_vbuckets == "all":
            target_vbuckets = None

        if op_type == DocLoading.Bucket.SubDocOps.INSERT:
            start = collection_obj.sub_doc_index[1]
            end = start + num_items
            collection_obj.sub_doc_index = (collection_obj.sub_doc_index[0],
                                            end)
            if target_vbuckets is not None:
                end -= start
            return sub_doc_generator(generic_key, start, end,
                                     target_vbucket=target_vbuckets, xattr_test=xattr_test)
        elif op_type == DocLoading.Bucket.SubDocOps.REMOVE:
            start = collection_obj.sub_doc_index[0]
            end = start + num_items
            collection_obj.sub_doc_index = (end,
                                            collection_obj.sub_doc_index[1])
            subdoc_gen_template_num = 2
        else:
            start = collection_obj.sub_doc_index[0]
            end = start + num_items
            subdoc_gen_template_num = choice([0, 1])

        if target_vbuckets is not None:
            end -= start
        return sub_doc_generator_for_edit(generic_key, start, end,
                                          subdoc_gen_template_num,
                                          target_vbucket=target_vbuckets, xattr_test=xattr_test)

    @staticmethod
    def perform_doc_loading_for_spec(task_manager,
                                     cluster,
                                     buckets,
                                     crud_spec,
                                     batch_size=200,
                                     async_load=False):
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
        :return: List of doc_loading tasks
        """
        loader_spec = dict()
        for bucket_name, scope_dict in crud_spec.items():
            bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
            loader_spec[bucket] = scope_dict
        task = task_manager.async_load_gen_docs_from_spec(
            cluster, task_manager.jython_task_manager, loader_spec,
            DocLoaderUtils.sdk_client_pool,
            batch_size=batch_size,
            process_concurrency=1,
            print_ops_rate=True,
            start_task=True)
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
                        target_ops = DocLoading.Bucket.DOC_OPS
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
                                        100)
                            else:
                                num_items = \
                                    DocLoaderUtils \
                                        .__get_required_num_from_percentage(
                                        collection,
                                        spec_percent_data[op_type])

                            if num_items == 0:
                                continue

                            c_crud_data[op_type] = dict()
                            c_crud_data[op_type]["doc_ttl"] = doc_ttl
                            c_crud_data[op_type]["target_items"] = num_items
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
                                        target_vbuckets=target_vbs,
                                        mutation_num=mutation_num,
                                        type=c_crud_data[op_type]["doc_gen_type"])
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

        doc_crud_spec = BucketUtils.get_random_collections(
            buckets,
            num_collection_to_load,
            num_scopes_to_consider,
            num_buckets_to_consider)
        sub_doc_crud_spec = BucketUtils.get_random_collections(
            buckets,
            num_collection_to_load,
            num_scopes_to_consider,
            num_buckets_to_consider)

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
    def validate_crud_task_per_collection(bucket, scope_name,
                                          collection_name, c_data):
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

        exception_pattern = ".*(com\.[a-zA-Z0-9\.]+)"

        # Fetch client for retry operations
        client = \
            DocLoaderUtils.sdk_client_pool.get_client_for_bucket(
                bucket, scope_name, collection_name)
        for op_type, op_data in c_data.items():
            failed_keys = op_data["fail"].keys()

            # New dicts to filter failures based on retry strategy
            op_data["unwanted"] = dict()
            op_data["retried"] = dict()
            op_data["unwanted"]["fail"] = dict()
            op_data["unwanted"]["success"] = dict()
            op_data["retried"]["fail"] = dict()
            op_data["retried"]["success"] = dict()

            if failed_keys:
                DocLoaderUtils.log.warning(
                    "%s:%s:%s %s failed keys from task: %s"
                    % (bucket.name, scope_name, collection_name,
                       op_type, failed_keys))
                for key, failed_doc in op_data["fail"].items():
                    is_key_to_ignore = False
                    exception = failed_doc["error"]
                    initial_exception = re.match(exception_pattern,
                                                 str(exception)).group(1)

                    for tem_exception in op_data["ignore_exceptions"]:
                        if str(exception).find(tem_exception) != -1:
                            if op_type == DocLoading.Bucket.DocOps.CREATE:
                                bucket.scopes[scope_name] \
                                    .collections[collection_name].num_items \
                                    -= 1
                            elif op_type == DocLoading.Bucket.DocOps.DELETE:
                                bucket.scopes[scope_name] \
                                    .collections[collection_name].num_items \
                                    += 1
                            is_key_to_ignore = True
                            break

                    if is_key_to_ignore:
                        op_data["fail"].pop(key)
                        ignored_keys_table.add_row([initial_exception, key])
                        continue

                    ambiguous_state = False
                    if SDKException.DurabilityAmbiguousException \
                            in str(exception) \
                            or SDKException.AmbiguousTimeoutException \
                            in str(exception) \
                            or SDKException.TimeoutException \
                            in str(exception) \
                            or (SDKException.RequestCanceledException
                                in str(exception) and
                                "CHANNEL_CLOSED_WHILE_IN_FLIGHT"
                                in str(exception)):
                        ambiguous_state = True

                    result = client.crud(
                        op_type, key, failed_doc["value"],
                        exp=op_data["doc_ttl"],
                        durability=op_data["durability_level"],
                        timeout=op_data["sdk_timeout"],
                        time_unit=op_data["sdk_timeout_unit"])

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
                                and SDKException.DocumentNotFoundException
                                in result["error"]
                                and op_type == "delete"):
                        op_data[retry_strategy]["success"][key] = \
                            result
                        if retry_strategy == "retried":
                            op_data["fail"].pop(key)
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

                gen_str = "%s:%s:%s - %s" \
                          % (bucket.name, scope_name, collection_name,
                             op_type)
                ignored_keys_table.display(
                    "%s keys ignored from retry" % gen_str)
                retry_succeeded_table.display(
                    "%s keys succeeded after expected retry" % gen_str)
                retry_failed_table.display(
                    "%s keys failed after expected retry" % gen_str)
                unwanted_retry_succeeded_table.display(
                    "%s unwanted exception keys succeeded in retry" % gen_str)
                unwanted_retry_failed_table.display(
                    "%s unwanted exception keys failed in retry" % gen_str)

        # Release the acquired client
        DocLoaderUtils.sdk_client_pool.release_client(client)

    @staticmethod
    def validate_doc_loading_results(doc_loading_task):
        """
        :param doc_loading_task: Task object returned from
                                 DocLoaderUtils.create_doc_gens_for_spec call
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
                        args=[bucket_obj, s_name, c_name, c_dict])
                    c_thread.start()
                    c_validation_threads.append(c_thread)

        # Wait for all threads to complete
        for c_thread in c_validation_threads:
            c_thread.join(60)

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
                               validate_task=True):
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
        :return doc_loading_task: Task object returned from
                                  DocLoaderUtils.create_doc_gens_for_spec call
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
            async_load=async_load)
        if not async_load and validate_task:
            DocLoaderUtils.validate_doc_loading_results(doc_loading_task)
        return doc_loading_task


class CollectionUtils(DocLoaderUtils):
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
        if collection:
            Collection.recreated(collection, collection_spec)
        else:
            collection = Collection(collection_spec)
            scope.collections[collection_name] = collection

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
        status, content = BucketHelper(node).create_collection(bucket,
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
                        collection_name=CbServer.default_collection, session=None):
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
        status, content = BucketHelper(node).delete_collection(bucket,
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
        status, content = BucketHelper(node).create_scope(bucket, scope_name, session=session)
        if status is False:
            ScopeUtils.log.error("Scope '%s:%s' creation failed: %s"
                                 % (bucket, scope_name, content))
            raise Exception("create_scope failed")

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
        status, content = BucketHelper(node).delete_scope(bucket,
                                                          scope_name,
                                                          session=session)
        if status is False:
            ScopeUtils.log.error("Scope '%s:%s' deletion failed: %s"
                                 % (bucket, scope_name, content))
            raise Exception("delete_scope failed")

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
    def __init__(self, cluster, cluster_util, server_task):
        self.cluster = cluster
        self.task = server_task
        self.task_manager = self.task.jython_task_manager
        self.cluster_util = cluster_util
        self.buckets = list()
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
                        max_length=CbServer.max_bucket_name_len):
        """
        API to generate random name which can be used to name
        a bucket/scope/collection
        """
        invalid_start_chars = "_%"
        spl_chars = "-"
        invalid_chars = "!@#$^&*()~`:;?/>.,<{}|\]["
        char_set = string.ascii_letters \
                   + string.digits \
                   + invalid_start_chars \
                   + spl_chars
        if invalid_name:
            char_set += invalid_chars

        # TODO: Remove once MB-43994 is fixed
        max_length = 30

        rand_name = ""
        now = datetime.datetime.now()
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
    def get_supported_durability_levels():
        return [key for key in vars(Bucket.DurabilityLevel)
                if not key.startswith("__")]

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
        selected_scopes = list()
        known_buckets = dict()
        exclude_scopes = list()

        for b_name, scope_dict in exclude_from.items():
            for scope_name in scope_dict["scopes"].keys():
                exclude_scopes.append("%s:%s" % (b_name, scope_name))

        for bucket_name in selected_buckets.keys():
            bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
            known_buckets[bucket_name] = bucket
            active_scopes = BucketUtils.get_active_scopes(bucket,
                                                          only_names=True)
            if consider_only_dropped:
                active_scopes = list(set(bucket.scopes.keys())
                                     .difference(set(active_scopes)))
            for scope in active_scopes:
                if scope == CbServer.default_scope and avoid_default:
                    continue
                available_scopes.append("%s:%s" % (bucket_name,
                                                   scope))

        if req_num == "all" or req_num >= len(available_scopes):
            selected_scopes = available_scopes
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
            for scope_name, collection_dict in scope_dict["scopes"].items():
                for c_name in collection_dict["collections"].keys():
                    exclude_collections.append("%s:%s:%s"
                                               % (b_name, scope_name, c_name))

        for bucket_name, scope_dict in selected_buckets.items():
            bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
            for scope_name in scope_dict["scopes"].keys():
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
            selected_collections = available_collections
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
    def load_sample_bucket(self, sample_bucket):
        bucket = None
        rest = RestConnection(self.cluster.master)
        api = '%s%s' % (rest.baseUrl, "sampleBuckets/install")
        data = '["%s"]' % sample_bucket.name
        status, _, _ = rest._http_request(api, "POST", data)
        sleep(5, "Wait before fetching buckets from cluster")
        buckets = self.get_all_buckets()
        for bucket in buckets:
            if bucket.name == sample_bucket.name:
                # Append loaded sample bucket into buckets object list
                self.buckets.append(bucket)
                break
        if status is True:
            warmed_up = self._wait_warmup_completed(
                self.cluster_util.get_kv_nodes(), bucket, wait_time=60)
            if not warmed_up:
                status = False
        if status is True:
            status = False
            retry_count = 120
            sleep_time = 5
            while retry_count > 0:
                item_count = self.get_buckets_itemCount()
                if item_count[sample_bucket.name] == \
                        sample_bucket.stats.expected_item_count:
                    status = True
                    break
                sleep(sleep_time, "Sample bucket still loading")
                retry_count -= sleep_time
        if status is False:
            self.log.error("Sample bucket failed to load the target items")

        return status

    def async_create_bucket(self, bucket):
        if not isinstance(bucket, Bucket):
            raise Exception("Create bucket needs Bucket object as parameter")
        self.log.debug("Creating bucket: %s" % bucket.name)
        task = BucketCreateTask(self.cluster.master, bucket)
        self.task_manager.add_new_task(task)
        return task

    def create_bucket(self, bucket, wait_for_warmup=True):
        raise_exception = None
        task = self.async_create_bucket(bucket)
        self.task_manager.get_task_result(task)
        if task.result is False:
            raise_exception = "BucketCreateTask failed"
        if task.result and wait_for_warmup:
            self.log.debug("Wait for memcached to accept cbstats "
                           "or any other bucket request connections")
            sleep(2)
            warmed_up = self._wait_warmup_completed(
                self.cluster_util.get_kv_nodes(), bucket, wait_time=60)
            if not warmed_up:
                task.result = False
                raise_exception = "Bucket not warmed up"

        if task.result:
            self.buckets.append(bucket)

        self.task_manager.stop_task(task)
        if raise_exception:
            raise_exception = "Create bucket %s failed: %s" \
                              % (bucket.name, raise_exception)
            raise Exception(raise_exception)

    def delete_bucket(self, serverInfo, bucket, wait_for_bucket_deletion=True):
        self.log.debug('Deleting existing bucket {0} on {1}'
                       .format(bucket, serverInfo))

        bucket_conn = BucketHelper(serverInfo)
        if self.bucket_exists(bucket):
            status = bucket_conn.delete_bucket(bucket.name)
            if not status:
                try:
                    self.print_dataStorage_content([serverInfo])
                    self.log.debug(StatsCommon.get_stats([serverInfo], bucket,
                                                         "timings"))
                except Exception as ex:
                    self.log.error("Unable to get timings for bucket: {0}"
                                   .format(ex))
            else:
                # Pop bucket object from self.buckets
                for index, t_bucket in enumerate(self.buckets):
                    if t_bucket.name == bucket.name:
                        self.buckets.pop(index)

            self.log.debug('Deleted bucket: {0} from {1}'
                           .format(bucket, serverInfo.ip))
        msg = 'Bucket "{0}" not deleted even after waiting for two minutes' \
            .format(bucket)
        if wait_for_bucket_deletion:
            if not self.wait_for_bucket_deletion(bucket, 200):
                try:
                    self.print_dataStorage_content([serverInfo])
                    self.log.debug(StatsCommon.get_stats([serverInfo], bucket,
                                                         "timings"))
                except Exception as ex:
                    self.log.error("Unable to get timings for bucket: {0}"
                                   .format(ex))
                self.log.error(msg)
                return False
            else:
                return True

    def wait_for_bucket_deletion(self, bucket,
                                 timeout_in_seconds=120):
        self.log.debug("Waiting for bucket %s deletion to finish"
                       % bucket.name)
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if not self.bucket_exists(bucket):
                return True
            else:
                sleep(2)
        return False

    def wait_for_bucket_creation(self, bucket,
                                 timeout_in_seconds=120):
        self.log.debug('Waiting for bucket creation to complete')
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if self.bucket_exists(bucket):
                return True
            else:
                sleep(2)
        return False

    def bucket_exists(self, bucket):
        for item in self.get_all_buckets(self.cluster.master):
            if item.name == bucket.name:
                return True
        return False

    def delete_all_buckets(self, servers=None):
        if servers is None:
            servers = self.cluster_util.get_kv_nodes()
        for serverInfo in servers:
            try:
                buckets = self.get_all_buckets(serverInfo)
            except Exception as e:
                self.log.error(e)
                sleep(10, "Wait before get_all_buckets() call")
                buckets = self.get_all_buckets(serverInfo)
            self.log.debug('Deleting existing buckets {0} on {1}'
                           .format([b.name for b in buckets], serverInfo.ip))
            for bucket in buckets:
                self.log.debug("Remove bucket {0} ...".format(bucket.name))
                try:
                    status = self.delete_bucket(serverInfo, bucket)
                except Exception as e:
                    self.log.error(e)
                    raise e
                if not status:
                    raise Exception("Bucket {0} could not be deleted"
                                    .format(bucket.name))

    def create_default_bucket(
            self, bucket_type=Bucket.Type.MEMBASE,
            ram_quota=None, replica=1, maxTTL=0,
            compression_mode="off", wait_for_warmup=True,
            conflict_resolution=Bucket.ConflictResolution.SEQ_NO,
            replica_index=1,
            storage=Bucket.StorageBackend.couchstore,
            eviction_policy=Bucket.EvictionPolicy.VALUE_ONLY,
            flush_enabled=Bucket.FlushBucket.DISABLED,
            bucket_durability=BucketDurability[Bucket.DurabilityLevel.NONE],
            purge_interval=1):
        node_info = RestConnection(self.cluster.master).get_nodes_self()
        if ram_quota:
            ram_quota_mb = ram_quota
        elif node_info.memoryQuota and int(node_info.memoryQuota) > 0:
            ram_available = node_info.memoryQuota
            ram_quota_mb = ram_available - 1
        else:
            # By default set 100Mb if unable to fetch proper value
            ram_quota_mb = 100

        default_bucket = Bucket({Bucket.bucketType: bucket_type,
                                 Bucket.ramQuotaMB: ram_quota_mb,
                                 Bucket.replicaNumber: replica,
                                 Bucket.compressionMode: compression_mode,
                                 Bucket.maxTTL: maxTTL,
                                 Bucket.conflictResolutionType:
                                     conflict_resolution,
                                 Bucket.replicaIndex: replica_index,
                                 Bucket.storageBackend: storage,
                                 Bucket.evictionPolicy: eviction_policy,
                                 Bucket.flushEnabled: flush_enabled,
                                 Bucket.durabilityMinLevel: bucket_durability,
                                 Bucket.purge_interval: purge_interval})
        self.create_bucket(default_bucket, wait_for_warmup)
        if self.enable_time_sync:
            self._set_time_sync_on_buckets([default_bucket.name])

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
        while collection_obj_index < req_num_collections:
            collection_name = BucketUtils.get_random_name(
                max_length=CbServer.max_collection_name_len)
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
        while scope_obj_index < req_num_scopes:
            scope_name = BucketUtils.get_random_name(
                max_length=CbServer.max_scope_name_len)
            if scope_name in \
                    scope_spec.keys():
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

        for _, bucket_spec in buckets_spec["buckets"].items():
            if Bucket.ramQuotaMB in bucket_spec:
                total_ram_requested_explicitly += \
                    bucket_spec[Bucket.ramQuotaMB]

        req_num_buckets = buckets_spec.get(MetaConstants.NUM_BUCKETS,
                                           len(buckets_spec["buckets"]))

        # Fetch and define RAM quota for buckets
        node_info = rest_conn.get_nodes_self()
        if Bucket.ramQuotaMB not in buckets_spec:
            if node_info.memoryQuota and int(node_info.memoryQuota) > 0:
                ram_available = node_info.memoryQuota
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
        while bucket_obj_index < req_num_buckets:
            bucket_name = BucketUtils.get_random_name()
            if bucket_name in buckets_spec["buckets"]:
                continue

            buckets_spec["buckets"][bucket_name] = dict()
            buckets_spec["buckets"][bucket_name]["scopes"] = dict()
            for param_name, param_value in bucket_defaults.items():
                buckets_spec["buckets"][bucket_name][param_name] = param_value

            # Expand scopes further within created bucket definition
            BucketUtils.expand_scope_spec(buckets_spec, bucket_name)
            bucket_obj_index += 1

        return buckets_spec["buckets"]

    def create_bucket_from_dict_spec(self, bucket_name, bucket_spec,
                                     async_create=True):
        task = BucketCreateFromSpecTask(self.task_manager,
                                        self.cluster.master,
                                        bucket_name,
                                        bucket_spec)
        self.task_manager.add_new_task(task)
        if not async_create:
            self.task_manager.get_task_result(task)
        return task

    def create_buckets_using_json_data(self, buckets_spec, async_create=True):
        self.log.info("Creating required buckets from template")
        rest_conn = RestConnection(self.cluster.master)
        buckets_spec = BucketUtils.expand_buckets_spec(rest_conn,
                                                       buckets_spec)
        bucket_creation_tasks = list()
        for bucket_name, bucket_spec in buckets_spec.items():
            bucket_creation_tasks.append(
                self.create_bucket_from_dict_spec(bucket_name, bucket_spec,
                                                  async_create=async_create))

        for task in bucket_creation_tasks:
            self.task_manager.get_task_result(task)
            if task.result is False:
                self.log.error("Failure in bucket creation task: %s"
                               % task.thread_name)
            else:
                self.buckets.append(task.bucket_obj)

        for bucket in self.buckets:
            for scope_name, scope_spec \
                    in buckets_spec[bucket.name]["scopes"].items():
                if type(scope_spec) is not dict:
                    continue

                if scope_name != CbServer.default_scope:
                    self.create_scope_object(bucket, scope_spec)

                for c_name, c_spec in scope_spec["collections"].items():
                    if type(c_spec) is not dict:
                        continue
                    c_spec["name"] = c_name
                    self.create_collection_object(bucket,
                                                  scope_name,
                                                  c_spec)

    # Support functions with bucket object
    @staticmethod
    def get_bucket_obj(buckets, bucket_name):
        bucket_obj = None
        for bucket in buckets:
            if bucket.name == bucket_name:
                bucket_obj = bucket
                break
        return bucket_obj

    def get_vbuckets(self, bucket='default'):
        b = self.get_bucket_obj(self.buckets, bucket)
        return None if not b else b.vbuckets

    def print_bucket_stats(self):
        table = TableView(self.log.info)
        table.set_headers(["Bucket", "Type", "Replicas", "Durability",
                           "TTL", "Items", "RAM Quota",
                           "RAM Used", "Disk Used"])
        buckets = self.get_all_buckets()
        if len(buckets) == 0:
            table.add_row(["No buckets", "", "", "", "", "", "", "", ""])
        else:
            for bucket in buckets:
                table.add_row(
                    [bucket.name, bucket.bucketType,
                     str(bucket.replicaNumber),
                     str(bucket.durability_level),
                     str(bucket.maxTTL),
                     str(bucket.stats.itemCount),
                     str(bucket.stats.ram),
                     str(bucket.stats.memUsed),
                     str(bucket.stats.diskUsed)])
        table.display("Bucket statistics")

    def get_vbucket_num_for_key(self, doc_key, total_vbuckets=1024):
        """
        Calculates vbucket number based on the document's key

        Argument:
        :doc_key        - Document's key
        :total_vbuckets - Total vbuckets present in the bucket

        Returns:
        :vbucket_number calculated based on the 'doc_key'
        """
        return (((zlib.crc32(doc_key)) >> 16) & 0x7fff) & (total_vbuckets - 1)

    def change_max_buckets(self, total_buckets):
        command = "curl -X POST -u {0}:{1} -d maxBucketCount={2} http://{3}:{4}/internalSettings" \
            .format(self.cluster.master.rest_username,
                    self.cluster.master.rest_password, total_buckets,
                    self.cluster.master.ip, self.cluster.master.port)
        shell = RemoteMachineShellConnection(self.cluster.master)
        output, error = shell.execute_command_raw(command)
        shell.log_command_output(output, error)
        shell.disconnect()

    def _set_time_sync_on_buckets(self, buckets):

        # get the credentials beforehand
        memcache_credentials = dict()
        for s in self.cluster.nodes_in_cluster:
            memcache_admin, memcache_admin_password = \
                RestConnection(s).get_admin_credentials()
            memcache_credentials[s.ip] = {'id': memcache_admin,
                                          'password': memcache_admin_password}

        for b in buckets:
            client1 = VBucketAwareMemcached(
                RestConnection(self.cluster.master), b)

            for j in range(b.vbuckets):
                active_vb = client1.memcached_for_vbucket(j)
                active_vb.sasl_auth_plain(
                    memcache_credentials[active_vb.host]['id'],
                    memcache_credentials[active_vb.host]['password'])
                active_vb.bucket_select(b)
                _ = active_vb.set_time_sync_state(j, 1)

    def get_bucket_compressionMode(self, bucket='default'):
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_info = bucket_helper.get_bucket_json(bucket=bucket)
        return bucket_info['compressionMode']

    def create_multiple_buckets(
            self, server, replica,
            bucket_ram_ratio=(2.0 / 3.0),
            bucket_count=3,
            bucket_type=Bucket.Type.MEMBASE,
            eviction_policy=Bucket.EvictionPolicy.VALUE_ONLY,
            maxttl=0,
            storage={Bucket.StorageBackend.couchstore: 3,
                     Bucket.StorageBackend.magma: 0},
            compression_mode=Bucket.CompressionMode.ACTIVE,
            bucket_durability=BucketDurability[Bucket.DurabilityLevel.NONE],
            ram_quota=None,
            bucket_name=None):
        success = True
        rest = RestConnection(server)
        info = rest.get_nodes_self()
        tasks = dict()
        if info.memoryQuota < 450.0:
            self.log.error("At least need 450MB memoryQuota")
            success = False
        else:
            if ram_quota is not None:
                bucket_ram = ram_quota
            else:
                available_ram = info.memoryQuota * bucket_ram_ratio
                if available_ram / bucket_count > 100:
                    bucket_ram = int(available_ram / bucket_count)
                else:
                    bucket_ram = 100
                # choose a port that is not taken by this ns server
            if type(storage) is not dict:
                storage = {storage: bucket_count}
            for key in storage.keys():
                count = 0
                while count < storage[key]:
                    name = "{0}.{1}".format(key, count)
                    if bucket_name is not None:
                        name= "{0}{1}".format(bucket_name, count)
                    bucket = Bucket({
                        Bucket.name: name,
                        Bucket.ramQuotaMB: bucket_ram,
                        Bucket.replicaNumber: replica,
                        Bucket.bucketType: bucket_type,
                        Bucket.evictionPolicy: eviction_policy,
                        Bucket.maxTTL: maxttl,
                        Bucket.storageBackend: key,
                        Bucket.compressionMode: compression_mode,
                        Bucket.durabilityMinLevel: bucket_durability})
                    tasks[bucket] = self.async_create_bucket(bucket)
                    count += 1

            raise_exception = None
            for bucket, task in tasks.items():
                self.task_manager.get_task_result(task)
                if task.result:
                    self.buckets.append(bucket)
                else:
                    raise_exception = "Create bucket %s failed" % bucket.name

            # Check for warm_up
            for bucket in self.buckets:
                warmed_up = self._wait_warmup_completed(
                    self.cluster_util.get_kv_nodes(), bucket, wait_time=60)
                if not warmed_up:
                    success = False
                    raise_exception = "Bucket %s not warmed up" % bucket.name

            if raise_exception:
                raise Exception(raise_exception)
        return success

    def flush_bucket(self, kv_node, bucket, skip_resetting_num_items=False):
        self.log.debug('Flushing existing bucket {0} on {1}'
                       .format(bucket, kv_node))
        bucket_conn = BucketHelper(kv_node)
        if self.bucket_exists(bucket):
            status = bucket_conn.flush_bucket(bucket)
            if not status:
                self.log.error("Flush bucket '{0}' failed from {1}"
                               .format(bucket, kv_node.ip))
            else:
                # Mark all existing collections as flushed
                for s_name in BucketUtils.get_active_scopes(bucket,
                                                            only_names=True):
                    for c_name in BucketUtils.get_active_collections(
                            bucket, s_name, only_names=True):
                        BucketUtils.mark_collection_as_flushed(bucket,
                                                               s_name,
                                                               c_name,
                                                               skip_resetting_num_items=skip_resetting_num_items)
            return status

    def flush_all_buckets(self, kv_node, skip_resetting_num_items=False):
        status = dict()
        self.log.debug("Flushing existing buckets '%s'"
                       % [bucket.name for bucket in self.buckets])
        for bucket in self.buckets:
            status[bucket] = self.flush_bucket(kv_node, bucket, skip_resetting_num_items=skip_resetting_num_items)
        return status

    def update_bucket_property(self, bucket, ram_quota_mb=None, auth_type=None,
                               sasl_password=None, replica_number=None,
                               replica_index=None, flush_enabled=None,
                               time_synchronization=None, max_ttl=None,
                               compression_mode=None, bucket_durability=None):
        BucketHelper(self.cluster.master).change_bucket_props(
            bucket, ramQuotaMB=ram_quota_mb, authType=auth_type,
            saslPassword=sasl_password, replicaNumber=replica_number,
            replicaIndex=replica_index, flushEnabled=flush_enabled,
            timeSynchronization=time_synchronization, maxTTL=max_ttl,
            compressionMode=compression_mode,
            bucket_durability=bucket_durability)

    def update_all_bucket_maxTTL(self, maxttl=0):
        for bucket in self.buckets:
            self.log.debug("Updating maxTTL for bucket %s to %ss"
                           % (bucket.name, maxttl))
            self.update_bucket_property(bucket, max_ttl=maxttl)

    def update_all_bucket_replicas(self, replicas=1):
        for bucket in self.buckets:
            self.log.debug("Updating replica for bucket %s to %ss"
                           % (bucket.name, replicas))
            self.update_bucket_property(bucket, replica_number=replicas)

    def is_warmup_complete(self, buckets, retry_count=5):
        buckets_warmed_up = True
        while retry_count != 0:
            buckets_warmed_up = True
            for bucket in buckets:
                try:
                    warmed_up = self._wait_warmup_completed(
                        self.cluster_util.get_kv_nodes(), bucket, wait_time=60)
                    if not warmed_up:
                        buckets_warmed_up = False
                        break
                except:
                    buckets_warmed_up = False
                    break
            retry_count -= 1
        return buckets_warmed_up

    def verify_cluster_stats(self, items, master=None,
                             timeout=None, check_items=True,
                             check_bucket_stats=True,
                             check_ep_items_remaining=False,
                             verify_total_items=True):
        if master is None:
            master = self.cluster.master
        self._wait_for_stats_all_buckets(
            timeout=(timeout or 120),
            check_ep_items_remaining=check_ep_items_remaining)
        if check_items:
            if check_bucket_stats:
                self.verify_stats_all_buckets(items=items,
                                              timeout=(timeout or 120))
            if verify_total_items:
                verified = True
                for bucket in self.buckets:
                    verified &= self.wait_till_total_numbers_match(
                        master, bucket, timeout_in_seconds=(timeout or 500))
                if not verified:
                    msg = "Lost items!!! Replication was completed " \
                          "but sum (curr_items) don't match the " \
                          "curr_items_total"
                    self.log.error(msg)
                    raise Exception(msg)

    def verify_stats_for_bucket(self, bucket, items, timeout=60):
        self.log.debug("Verifying stats for bucket {0}".format(bucket.name))
        stats_tasks = []
        servers = self.cluster_util.get_kv_nodes()
        if bucket.bucketType == Bucket.Type.MEMCACHED:
            items_actual = 0
            for server in servers:
                client = MemcachedClientHelper.direct_client(server, bucket)
                items_actual += int(client.stats()["curr_items"])
            if items != items_actual:
                raise Exception("Items are not correct")
            return

        # TODO: Need to fix the config files to always satisfy the
        #       replica number based on the available number_of_servers
        available_replicas = bucket.replicaNumber
        if len(servers) == bucket.replicaNumber:
            available_replicas = len(servers) - 1
        elif len(servers) <= bucket.replicaNumber:
            available_replicas = len(servers) - 1

        # Create connection to master node for verifying cbstats
        stat_cmd = "all"
        shell_conn_list1 = list()
        shell_conn_list2 = list()
        for cluster_node in servers:
            shell_conn_list1.append(
                RemoteMachineShellConnection(cluster_node))
            shell_conn_list2.append(
                RemoteMachineShellConnection(cluster_node))

        # Create Tasks to verify total items/replica count in the bucket
        stats_tasks.append(self.task.async_wait_for_stats(
            shell_conn_list1, bucket, stat_cmd,
            'vb_replica_curr_items', '==', items * available_replicas,
            timeout=timeout))
        stats_tasks.append(self.task.async_wait_for_stats(
            shell_conn_list2, bucket, stat_cmd,
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

        for remote_conn in shell_conn_list1 + shell_conn_list2:
            remote_conn.disconnect()

    def verify_stats_all_buckets(self, items, timeout=500):
        vbucket_stats = self.get_vbucket_seqnos(
            self.cluster_util.get_kv_nodes(),
            self.buckets,
            skip_consistency=True)
        for bucket in self.buckets:
            self.verify_stats_for_bucket(bucket, items, timeout=timeout)
            # Validate seq_no snap_start/stop values with initial load
            result = self.validate_seq_no_stats(vbucket_stats[bucket.name])
            self.assertTrue(result,
                            "snap_start and snap_end corruption found!!!")

    def _wait_for_stat(self, bucket, stat_map=None,
                       cbstat_cmd="checkpoint",
                       stat_name="num_items_for_persistence",
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
                shell_conn = RemoteMachineShellConnection(server)
                tasks.append(self.task.async_wait_for_stats(
                    [shell_conn], bucket, cbstat_cmd,
                    stat_name, stat_cond, stat_value,
                    timeout=timeout))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            for shell in task.shellConnList:
                shell.disconnect()

    def _wait_for_stats_all_buckets(self, expected_val=0,
                                    comparison_condition='==',
                                    check_ep_items_remaining=False,
                                    timeout=500,
                                    cbstat_cmd="checkpoint",
                                    stat_name="num_items_for_persistence"):
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
        for server in self.cluster_util.get_kv_nodes():
            for bucket in self.buckets:
                if bucket.bucketType == 'memcached':
                    continue
                shell_conn = RemoteMachineShellConnection(server)
                tasks.append(self.task.async_wait_for_stats(
                    [shell_conn], bucket, cbstat_cmd,
                    stat_name, comparison_condition, expected_val,
                    timeout=timeout))
                if check_ep_items_remaining:
                    dcp_cmd = "dcp"
                    shell_conn = RemoteMachineShellConnection(server)
                    tasks.append(self.task.async_wait_for_stats(
                        [shell_conn], bucket, dcp_cmd,
                        'ep_dcp_items_remaining', "==", 0,
                        timeout=timeout))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            for shell in task.shellConnList:
                shell.disconnect()

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

    def wait_for_collection_creation_to_complete(self, timeout=60):
        self.log.info("Waiting for all collections to be created")
        bucket_helper = BucketHelper(self.cluster.master)
        for bucket in self.buckets:
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

    def get_doc_op_info_dict(self, bucket, op_type, exp=0, replicate_to=0,
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

    def doc_ops_tasks_status(self, tasks_info):
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
        for _, task_info in tasks_info.items():
            op_type = task_info["op_type"]
            ignored_keys = task_info["ignored"].keys()
            retried_success_keys = task_info["retried"]["success"].keys()
            retried_failed_keys = task_info["retried"]["fail"].keys()
            unwanted_success_keys = task_info["unwanted"]["success"].keys()
            unwanted_failed_keys = task_info["unwanted"]["fail"].keys()

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
                self.log.error("Docs failed after expected retry "
                               "for '{0}' ({1}): {2}"
                               .format(op_type, len(retried_failed_keys),
                                       retried_failed_keys))
                self.log.error("Exceptions for failure on retried docs: {0}"
                               .format(task_info["retried"]["fail"]))

            if len(unwanted_success_keys) > 0:
                task_info["ops_failed"] = True
                self.log.error("Unexpected exceptions, succeeded "
                               "after retry for '{0}' ({1}): {2}"
                               .format(op_type, len(unwanted_success_keys),
                                       unwanted_success_keys))

            if len(unwanted_failed_keys) > 0:
                task_info["ops_failed"] = True
                self.log.error("Unexpected exceptions, failed even "
                               "after retry for '{0}' ({1}): {2}"
                               .format(op_type, len(unwanted_failed_keys),
                                       unwanted_failed_keys))
                self.log.error("Exceptions for unwanted doc failures "
                               "after retry: {0}"
                               .format(task_info["unwanted"]["fail"]))

    def verify_doc_op_task_exceptions(self, tasks_info, cluster,
                                      sdk_client_pool=None):
        """
        :param tasks_info:  dict() of dict() of form,
                            tasks_info[task_obj] = get_doc_op_info_dict()
        :param cluster:     Cluster object
        :param sdk_client_pool: Instance of SDKClientPool
        :return: tasks_info dictionary updated with retried/unwanted docs
        """
        for task, task_info in tasks_info.items():
            client = None
            bucket = task_info["bucket"]
            scope = task_info["scope"]
            collection = task_info["collection"]

            if sdk_client_pool:
                client = sdk_client_pool.get_client_for_bucket(bucket,
                                                               scope,
                                                               collection)

            if client is None:
                client = SDKClient([cluster.master],
                                   bucket,
                                   scope=scope,
                                   collection=collection)

            for key, failed_doc in task.fail.items():
                found = False
                exception = failed_doc["error"]
                key_value = {key: failed_doc}
                for ex in task_info["ignore_exceptions"]:
                    if str(exception).find(ex) != -1:
                        bucket \
                            .scopes[scope] \
                            .collections[collection].num_items -= 1
                        tasks_info[task]["ignored"].update(key_value)
                        found = True
                        break
                if found:
                    continue

                ambiguous_state = False
                if SDKException.DurabilityAmbiguousException \
                        in str(exception) \
                        or SDKException.AmbiguousTimeoutException \
                        in str(exception) \
                        or SDKException.TimeoutException \
                        in str(exception) \
                        or SDKException.RequestCanceledException \
                        in str(exception):
                    ambiguous_state = True

                result = client.crud(
                    task_info["op_type"], key, failed_doc["value"],
                    exp=task_info["exp"],
                    replicate_to=task_info["replicate_to"],
                    persist_to=task_info["persist_to"],
                    durability=task_info["durability"],
                    timeout=task_info["timeout"],
                    time_unit=task_info["time_unit"])

                dict_key = "unwanted"
                for ex in task_info["retry_exceptions"]:
                    if str(exception).find(ex) != -1:
                        dict_key = "retried"
                        break
                if result["status"] \
                        or (ambiguous_state
                            and SDKException.DocumentExistsException
                            in result["error"]
                            and task_info["op_type"] in ["create", "update"]) \
                        or (ambiguous_state
                            and SDKException.DocumentNotFoundException
                            in result["error"]
                            and task_info["op_type"] == "delete"):
                    tasks_info[task][dict_key]["success"].update(key_value)
                else:
                    tasks_info[task][dict_key]["fail"].update(key_value)
            if sdk_client_pool:
                sdk_client_pool.release_client(client)
            else:
                # Close client for this task
                client.close()

        return tasks_info

    def async_load_bucket(self, cluster, bucket, generator, op_type,
                          exp=0, random_exp=False,
                          flag=0, persist_to=0, replicate_to=0,
                          durability="", sdk_timeout=5,
                          only_store_hash=True, batch_size=10, pause_secs=1,
                          compression=True, process_concurrency=8, retries=5,
                          active_resident_threshold=100,
                          ryow=False, check_persistence=False,
                          skip_read_on_error=False, suppress_error_table=False,
                          dgm_batch=5000,
                          scope=CbServer.default_scope,
                          collection=CbServer.default_collection,
                          monitor_stats=["doc_ops"],
                          track_failures=True,
                          sdk_client_pool=None):
        return self.task.async_load_gen_docs(
            cluster, bucket, generator, op_type,
            exp=exp, random_exp=random_exp,
            flag=flag, persist_to=persist_to, replicate_to=replicate_to,
            durability=durability, timeout_secs=sdk_timeout,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs, compression=compression,
            process_concurrency=process_concurrency, retries=retries,
            active_resident_threshold=active_resident_threshold,
            ryow=ryow, check_persistence=check_persistence,
            suppress_error_table=suppress_error_table,
            skip_read_on_error=skip_read_on_error,
            dgm_batch=dgm_batch,
            scope=scope, collection=collection,
            monitor_stats=monitor_stats,
            track_failures=track_failures,
            sdk_client_pool=sdk_client_pool)

    def _async_load_all_buckets(self, cluster, kv_gen, op_type,
                                exp, random_exp=False,
                                flag=0, persist_to=0, replicate_to=0,
                                only_store_hash=True, batch_size=1,
                                pause_secs=1, timeout_secs=30,
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
                                sdk_client_pool=None):

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
        for bucket in self.buckets:
            task = self.async_load_bucket(
                cluster, bucket, kv_gen, op_type, exp, random_exp,
                flag, persist_to,
                replicate_to, durability, timeout_secs,
                only_store_hash, batch_size, pause_secs,
                sdk_compression, process_concurrency, retries,
                active_resident_threshold=active_resident_threshold,
                ryow=ryow, check_persistence=check_persistence,
                suppress_error_table=suppress_error_table,
                skip_read_on_error=skip_read_on_error,
                dgm_batch=dgm_batch,
                scope=scope, collection=collection,
                monitor_stats=monitor_stats,
                track_failures=track_failures,
                sdk_client_pool=sdk_client_pool)
            tasks_info[task] = self.get_doc_op_info_dict(
                bucket, op_type, exp,
                scope=scope,
                collection=collection,
                replicate_to=replicate_to,
                persist_to=persist_to,
                durability=durability,
                timeout=timeout_secs, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
        return tasks_info

    def _async_validate_docs(self, cluster, kv_gen, op_type, exp,
                             flag=0, only_store_hash=True, batch_size=1,
                             pause_secs=1, timeout_secs=5, compression=True,
                             process_concurrency=4, check_replica=False,
                             ignore_exceptions=[], retry_exceptions=[],
                             scope=CbServer.default_scope,
                             collection=CbServer.default_collection,
                             suppress_error_table=False,
                             sdk_client_pool=None):
        task_info = dict()
        for bucket in self.buckets:
            gen = copy.deepcopy(kv_gen)
            task = self.task.async_validate_docs(
                cluster, bucket, gen, op_type, exp, flag, only_store_hash,
                batch_size, pause_secs, timeout_secs, compression,
                process_concurrency, check_replica,
                scope, collection,
                suppress_error_table=suppress_error_table,
                sdk_client_pool=sdk_client_pool)
            task_info[task] = self.get_doc_op_info_dict(
                bucket, op_type, exp,
                scope=scope,
                collection=collection,
                timeout=timeout_secs, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            return task_info

    def sync_load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                              persist_to=0, replicate_to=0,
                              only_store_hash=True, batch_size=1,
                              pause_secs=1, timeout_secs=30,
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
                              sdk_client_pool=None):

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
            cluster, kv_gen, op_type, exp, flag,
            persist_to, replicate_to,
            only_store_hash, batch_size, pause_secs, timeout_secs,
            sdk_compression, process_concurrency, retries, durability,
            ignore_exceptions, retry_exceptions, ryow=ryow,
            active_resident_threshold=active_resident_threshold,
            check_persistence=check_persistence,
            skip_read_on_error=skip_read_on_error,
            suppress_error_table=suppress_error_table,
            dgm_batch=dgm_batch,
            scope=scope, collection=collection,
            monitor_stats=monitor_stats,
            track_failures=track_failures,
            sdk_client_pool=sdk_client_pool)

        for task in tasks_info.keys():
            self.task_manager.get_task_result(task)

        # Wait for all doc_loading tasks to complete and populate failures
        self.verify_doc_op_task_exceptions(tasks_info, cluster,
                                           sdk_client_pool=sdk_client_pool)
        self.log_doc_ops_task_failures(tasks_info)
        return tasks_info

    def load_durable_aborts(self, ssh_shell, load_gens, bucket,
                            durability_level, doc_op="create",
                            load_type="all_aborts"):
        """
        :param ssh_shell: ssh connection for simulating memcached_stop
        :param load_gens: doc_load generators used for running doc_ops
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
            if doc_gen.doc_keys:
                return len(doc_gen.doc_keys)
            return doc_gen.end

        def load_docs(doc_gen, load_for=""):
            if load_for == "abort":
                return self.task.async_load_gen_docs(
                    self.cluster, bucket, doc_gen, doc_op, 0,
                    batch_size=10, process_concurrency=8,
                    durability=durability_level,
                    timeout_secs=2, start_task=False,
                    skip_read_on_error=True, suppress_error_table=True)
            return self.task.async_load_gen_docs(
                self.cluster, bucket, doc_gen, doc_op, 0,
                batch_size=10, process_concurrency=8,
                durability=durability_level,
                timeout_secs=60)

        result = True
        tasks = list()
        num_items = dict()
        cb_err = CouchbaseError(self.log, ssh_shell)

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

    def verify_unacked_bytes_all_buckets(self, filter_list=[]):
        """
        Waits for max_unacked_bytes = 0 on all servers and buckets in a cluster
        A utility function that waits upr flow with unacked_bytes = 0
        """
        servers = self.cluster_util.get_kv_nodes()
        dcp_stat_map = self.data_collector.collect_compare_dcp_stats(
            self.buckets, servers, filter_list=filter_list)
        for bucket in dcp_stat_map.keys():
            if dcp_stat_map[bucket]:
                self.log.critical("Bucket {0} has unacked bytes != 0: {1}"
                                  .format(bucket, dcp_stat_map[bucket]))

    def disable_compaction(self, server=None, bucket="default"):
        new_config = {"viewFragmntThresholdPercentage": None,
                      "dbFragmentThresholdPercentage": None,
                      "dbFragmentThreshold": None,
                      "viewFragmntThreshold": None}
        self.modify_fragmentation_config(new_config, bucket)

    def modify_fragmentation_config(self, config, bucket="default"):
        bucket_op = BucketHelper(self.cluster.master)
        _config = {"parallelDBAndVC": "false",
                   "dbFragmentThreshold": None,
                   "viewFragmntThreshold": None,
                   "dbFragmentThresholdPercentage": 100,
                   "viewFragmntThresholdPercentage": 100,
                   "allowedTimePeriodFromHour": None,
                   "allowedTimePeriodFromMin": None,
                   "allowedTimePeriodToHour": None,
                   "allowedTimePeriodToMin": None,
                   "allowedTimePeriodAbort": None,
                   "autoCompactionDefined": "true"}
        _config.update(config)
        bucket_op.set_auto_compaction(
            parallelDBAndVC=_config["parallelDBAndVC"],
            dbFragmentThreshold=_config["dbFragmentThreshold"],
            viewFragmntThreshold=_config["viewFragmntThreshold"],
            dbFragmentThresholdPercentage=_config["dbFragmentThresholdPercentage"],
            viewFragmntThresholdPercentage=_config["viewFragmntThresholdPercentage"],
            allowedTimePeriodFromHour=_config["allowedTimePeriodFromHour"],
            allowedTimePeriodFromMin=_config["allowedTimePeriodFromMin"],
            allowedTimePeriodToHour=_config["allowedTimePeriodToHour"],
            allowedTimePeriodToMin=_config["allowedTimePeriodToMin"],
            allowedTimePeriodAbort=_config["allowedTimePeriodAbort"],
            bucket=bucket)

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

    def get_vbucket_seqnos_per_Node_Only(self, servers, buckets):
        """
        Method to get vbucket information from a cluster using cbstats
        """
        servers = self.cluster_util.get_kv_nodes(servers)
        new_vbucket_stats = self.data_collector.collect_vbucket_stats(
            buckets, servers, collect_vbucket=False,
            collect_vbucket_seqno=True, collect_vbucket_details=False,
            perNode=True)
        self.compare_per_node_for_vbucket_consistency(new_vbucket_stats)
        return new_vbucket_stats

    def compare_vbucket_seqnos(self, prev_vbucket_stats, servers, buckets,
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
            new_vbucket_stats = self.get_vbucket_seqnos_per_Node_Only(servers,
                                                                      buckets)
        else:
            new_vbucket_stats = self.get_vbucket_seqnos(servers, buckets)
        isNotSame = True
        summary = ""
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

    def compare_per_node_for_vbucket_consistency(self, map1, check_abs_high_seqno=False, check_purge_seqno=False):
        """
            Method to check uuid is consistent on active and replica new_vbucket_stats
        """
        bucketMap = dict()
        logic = True
        for bucket in map1.keys():
            map = dict()
            nodeMap = dict()
            output = ""
            for node in map1[bucket].keys():
                for vbucket in map1[bucket][node].keys():
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

    def compare_vbucketseq_failoverlogs(self, vbucketseq={}, failoverlog={}):
        """
            Method to compare failoverlog and vbucket-seq for uuid and  seq no
        """
        isTrue = True
        output = ""
        for bucket in vbucketseq.keys():
            for vbucket in vbucketseq[bucket].keys():
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

    def print_results_per_node(self, node_map):
        """ Method to print map results - Used only for debugging purpose """
        for bucket in node_map.keys():
            print("----- Bucket {0} -----".format(bucket))
            for node in node_map[bucket].keys():
                print("-------------Node {0}------------".format(node))
                for vbucket in node_map[bucket][node].keys():
                    print("   for vbucket {0}".format(vbucket))
                    for key in node_map[bucket][node][vbucket].keys():
                        print("            :: for key {0} = {1}"
                              .format(key,
                                      node_map[bucket][node][vbucket][key]))

    def vb_distribution_analysis(self, servers=[], buckets=[], num_replicas=0,
                                 total_vbuckets=0, std=1.0, type="rebalance",
                                 graceful=True):
        """
        Method to check vbucket distribution analysis after rebalance
        """
        self.log.debug("Begin Verification for vb_distribution_analysis")
        servers = self.cluster_util.get_kv_nodes(servers)
        active, replica = self.get_vb_distribution_active_replica(
            servers=servers, buckets=buckets)
        for bucket in active.keys():
            self.log.debug("Begin Verification for Bucket {0}".format(bucket))
            active_result = active[bucket]
            replica_result = replica[bucket]
            if graceful or type == "rebalance":
                self.assertTrue(active_result["total"] == total_vbuckets,
                                "total vbuckets do not match for active data set (= criteria), actual {0} expectecd {1}"
                                .format(active_result["total"], total_vbuckets))
            else:
                self.assertTrue(active_result["total"] <= total_vbuckets,
                                "total vbuckets do not match for active data set  (<= criteria), actual {0} expectecd {1}"
                                .format(active_result["total"], total_vbuckets))
            if type == "rebalance":
                rest = RestConnection(self.cluster.master)
                nodes = rest.node_statuses()
                if (len(nodes) - num_replicas) >= 1:
                    self.assertTrue(replica_result["total"] == num_replicas * total_vbuckets,
                                    "total vbuckets do not match for replica data set (= criteria), actual {0} expected {1}"
                                    .format(replica_result["total"], num_replicas * total_vbuckets))
                else:
                    self.assertTrue(replica_result["total"] < num_replicas * total_vbuckets,
                                    "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}"
                                    .format(replica_result["total"], num_replicas * total_vbuckets))
            else:
                self.assertTrue(replica_result["total"] <= num_replicas * total_vbuckets,
                                "total vbuckets do not match for replica data set (<= criteria), actual {0} expected {1}"
                                .format(replica_result["total"], num_replicas * total_vbuckets))
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
        servers = self.cluster_util.get_kv_nodes(servers)
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

    def get_data_set_with_data_distribution_all(self, servers, buckets,
                                                path=None, mode="disk"):
        """ Method to get all data set for buckets and from the servers """
        servers = self.cluster_util.get_kv_nodes(servers)
        _, dataset = self.data_collector.collect_data(
            servers, buckets, data_path=path, perNode=False, mode=mode)
        distribution = self.data_analyzer.analyze_data_distribution(dataset)
        return dataset, distribution

    def get_vb_distribution_active_replica(self, servers=[], buckets=[]):
        """
        Method to distribution analysis for active and replica vbuckets
        """
        servers = self.cluster_util.get_kv_nodes(servers)
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

    def compare_per_node_for_failovers_consistency(self, map1, vbucketMap):
        """
        Method to check uuid is consistent on active and
        replica new_vbucket_stats
        """
        bucketMap = dict()
        logic = True
        for bucket in map1.keys():
            map = dict()
            tempMap = dict()
            output = ""
            for node in map1[bucket].keys():
                for vbucket in map1[bucket][node].keys():
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

    def compare_failovers_logs(self, prev_failovers_stats, servers, buckets,
                               perNode=False, comp_map=None):
        """
        Method to compare failover log information to a previously stored value
        """
        comp_map = dict()
        comp_map["id"] = {'type': "string", 'operation': "=="}
        comp_map["seq"] = {'type': "long", 'operation': "<="}
        comp_map["num_entries"] = {'type': "string", 'operation': "<="}

        self.log.debug("Begin Verification for failovers logs comparison")
        servers = self.cluster_util.get_kv_nodes(servers)
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
                             verify_data=True, exp=0, num_items=0):
        for key in docs_gen_map.keys():
            if key != "remaining":
                op_type = key
                if key == "expiry":
                    op_type = "update"
                    verify_data = False
                    self.expiry = 3
                self.sync_load_all_buckets(cluster, docs_gen_map[key][0],
                                           op_type, exp)
                if verify_data:
                    self.verify_cluster_stats(num_items)
        if "expiry" in docs_gen_map.keys():
            self._expiry_pager()

    def async_ops_all_buckets(self, docs_gen_map={}, batch_size=10):
        tasks = []
        if "expiry" in docs_gen_map.keys():
            self._expiry_pager()
        for key in docs_gen_map.keys():
            if key != "remaining":
                op_type = key
                if key == "expiry":
                    op_type = "update"
                    self.expiry = 3
                tasks += self.async_load(docs_gen_map[key], op_type=op_type,
                                         exp=self.expiry,
                                         batch_size=batch_size)
        return tasks

    def _expiry_pager(self, val=10):
        for node in self.cluster_util.get_kv_nodes():
            shell_conn = RemoteMachineShellConnection(node)
            cbepctl_obj = Cbepctl(shell_conn)
            for bucket in self.buckets:
                cbepctl_obj.set(bucket.name,
                                "flush_param",
                                "exp_pager_stime",
                                val)
            shell_conn.disconnect()

    def _run_compaction(self, number_of_times=100):
        try:
            for _ in range(0, number_of_times):
                for bucket in self.buckets:
                    BucketHelper(self.cluster.master).compact_bucket(
                        bucket.name)
        except Exception, ex:
            self.log.error(ex)

    def _load_data_in_buckets_using_mc_bin_client(self, bucket, data_set,
                                                  max_expiry_range=None):
        client = VBucketAwareMemcached(RestConnection(self.cluster.master),
                                       bucket)
        try:
            for key in data_set.keys():
                expiry = 0
                if max_expiry_range is not None:
                    expiry = random.randint(1, max_expiry_range)
                o, c, d = client.set(key, expiry, 0, json.dumps(data_set[key]))
        except Exception as ex:
            print('Exception: {0}'.format(ex))

    def run_mc_bin_client(self, number_of_times=500000, max_expiry_range=30):
        data_map = dict()
        for i in range(number_of_times):
            name = "key_" + str(i) + str((random.randint(1, 10000))) + \
                   str((random.randint(1, 10000)))
            data_map[name] = {"name": "none_the_less"}
        for bucket in self.buckets:
            try:
                self._load_data_in_buckets_using_mc_bin_client(
                    bucket, data_map, max_expiry_range)
            except Exception, ex:
                self.log.error(ex)

    def get_item_count_mc(self, server, bucket):
        client = MemcachedClientHelper.direct_client(server, bucket)
        return int(client.stats()["curr_items"])

    def get_bucket_current_item_count(self, cluster, bucket):
        bucket_map = self.get_buckets_itemCount(cluster)
        return bucket_map[bucket.name]

    def get_buckets_itemCount(self, cluster=None):
        if not cluster:
            return BucketHelper(self.cluster.master).get_buckets_itemCount()
        return BucketHelper(cluster.master).get_buckets_itemCount()

    def expire_pager(self, servers, val=10):
        for bucket in self.buckets:
            for server in servers:
                ClusterOperationHelper.flushctl_set(server, "exp_pager_stime",
                                                    val, bucket)
        sleep(val, "Wait for expiry pager to run on all these nodes")

    def set_auto_compaction(self, bucket_helper,
                            parallelDBAndVC="false",
                            dbFragmentThreshold=None,
                            viewFragmntThreshold=None,
                            dbFragmentThresholdPercentage=None,
                            viewFragmntThresholdPercentage=None,
                            allowedTimePeriodFromHour=None,
                            allowedTimePeriodFromMin=None,
                            allowedTimePeriodToHour=None,
                            allowedTimePeriodToMin=None,
                            allowedTimePeriodAbort=None,
                            bucket=None):
        output, rq_content, _ = bucket_helper.set_auto_compaction(
            parallelDBAndVC, dbFragmentThreshold, viewFragmntThreshold,
            dbFragmentThresholdPercentage, viewFragmntThresholdPercentage,
            allowedTimePeriodFromHour, allowedTimePeriodFromMin,
            allowedTimePeriodToHour, allowedTimePeriodToMin,
            allowedTimePeriodAbort, bucket)

        if not output and (dbFragmentThresholdPercentage, dbFragmentThreshold,
                           viewFragmntThresholdPercentage,
                           viewFragmntThreshold <= MIN_COMPACTION_THRESHOLD
                           or dbFragmentThresholdPercentage,
                           viewFragmntThresholdPercentage >= MAX_COMPACTION_THRESHOLD):
            self.assertFalse(output,
                             "Should be impossible to set compaction val {0}%"
                             .format(viewFragmntThresholdPercentage))
            self.assertTrue("errors" in json.loads(rq_content),
                            "Error is not present in response")
            self.assertTrue(str(json.loads(rq_content)["errors"])
                            .find("Allowed range is 2 - 100") > -1,
                            "Error 'Allowed range is 2 - 100', but was '{0}'"
                            .format(str(json.loads(rq_content)["errors"])))
            self.log.debug("Response contains error = '%(errors)s' as expected"
                           % json.loads(rq_content))
        return output, rq_content

    def get_bucket_priority(self, priority):
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
        rest = RestConnection(node)
        bucket_conn = BucketHelper(node)
        bucket_conn.vbucket_map_ready(bucket, 60)
        vbucket_count = len(bucket.vbuckets)
        vbuckets = bucket.vbuckets
        obj = VBucketAwareMemcached(rest, bucket, info=node)
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
                    except exceptions.EOFError:
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
                        client.sasl_auth_plain(
                            bucket.name.encode('ascii'),
                            bucket.saslPassword.encode('ascii'))
                        continue

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

    def fetch_available_memory_for_kv_on_a_node(self):
        """
        Calculates the Memory that can be allocated for KV service on a node
        :return: Memory that can be used for KV service.
        """
        info = RestConnection(self.cluster.master).get_nodes_self()
        free_memory_in_mb = info.memoryFree // 1024 ** 2
        total_available_memory_in_mb = 0.8 * free_memory_in_mb

        active_service = info.services
        if "index" in active_service:
            total_available_memory_in_mb -= info.indexMemoryQuota
        if "fts" in active_service:
            total_available_memory_in_mb -= info.ftsMemoryQuota
        if "cbas" in active_service:
            total_available_memory_in_mb -= info.cbasMemoryQuota
        if "eventing" in active_service:
            total_available_memory_in_mb -= info.eventingMemoryQuota

        return total_available_memory_in_mb

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

    def get_all_buckets(self, server=None):
        if server is None:
            server = self.cluster.master
        rest = BucketHelper(server)
        json_parsed = rest.get_buckets_json()
        bucket_list = list()
        for item in json_parsed:
            bucket_list.append(self.parse_get_bucket_json(item))
        return bucket_list

    def get_fragmentation_kv(self, bucket=None, server=None):
        if bucket is None:
            bucket = self.buckets[0]
        if server is None:
            server = self.cluster.master
        bucket_helper = BucketHelper(server)
        stats = bucket_helper.fetch_bucket_stats(bucket.name)
        frag_val = float(stats["op"]["samples"]["couch_docs_fragmentation"][-1])
        return frag_val

    def parse_get_bucket_json(self, parsed):
        bucket = None
        for bucket in self.buckets:
            if bucket.name == parsed['name']:
                break

        if bucket is None:
            bucket = Bucket()
            bucket.name = parsed["name"]

        bucket.uuid = parsed['uuid']
        bucket.bucketType = parsed['bucketType']
        if str(parsed['bucketType']) == 'membase':
            bucket.bucketType = Bucket.Type.MEMBASE
        bucket.authType = parsed["authType"]
        bucket.saslPassword = parsed["saslPassword"]
        bucket.nodes = list()
        if "maxTTL" in parsed:
            bucket.maxTTL = parsed["maxTTL"]
        bucket.durability_level = "none"
        bucket.bucketCapabilities = parsed["bucketCapabilities"]
        if Bucket.durabilityMinLevel in parsed:
            bucket.durability_level = parsed[Bucket.durabilityMinLevel]
        if 'vBucketServerMap' in parsed:
            vBucketServerMap = parsed['vBucketServerMap']
            serverList = vBucketServerMap['serverList']
            bucket.servers.extend(serverList)
            if "numReplicas" in vBucketServerMap:
                bucket.replicaNumber = vBucketServerMap["numReplicas"]
            if "serverList" in vBucketServerMap:
                bucket.replicaServers = vBucketServerMap["serverList"]
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
        quota = parsed['quota']
        bucket.stats.ram = quota['ram']
        nodes = parsed['nodes']
        for nodeDictionary in nodes:
            node = Node()
            node.uptime = nodeDictionary['uptime']
            node.memoryFree = nodeDictionary['memoryFree']
            node.memoryTotal = nodeDictionary['memoryTotal']
            node.mcdMemoryAllocated = nodeDictionary['mcdMemoryAllocated']
            node.mcdMemoryReserved = nodeDictionary['mcdMemoryReserved']
            node.status = nodeDictionary['status']
            node.hostname = nodeDictionary['hostname']
            if 'clusterCompatibility' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterCompatibility']
            if 'clusterMembership' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterMembership']
            node.version = nodeDictionary['version']
            node.os = nodeDictionary['os']
            if "ports" in nodeDictionary:
                ports = nodeDictionary["ports"]
                if "proxy" in ports:
                    node.moxi = ports["proxy"]
                if "direct" in ports:
                    node.memcached = ports["direct"]
            if "hostname" in nodeDictionary:
                value = str(nodeDictionary["hostname"])
                node.ip = value[:value.rfind(":")]
                node.port = int(value[value.rfind(":") + 1:])
            if "otpNode" in nodeDictionary:
                node.id = nodeDictionary["otpNode"]
            bucket.nodes.append(node)
        return bucket

    def wait_till_total_numbers_match(self, master, bucket,
                                      timeout_in_seconds=120):
        self.log.debug('Waiting for sum_of_curr_items == total_items')
        start = time.time()
        verified = False
        while (time.time() - start) <= timeout_in_seconds:
            try:
                if self.verify_items_count(master, bucket):
                    verified = True
                    break
                else:
                    sleep(2)
            except StatsUnavailableException:
                self.log.error("Unable to retrieve stats for any node!")
                break
        if not verified:
            rest = RestConnection(master)
            RebalanceHelper.print_taps_from_all_nodes(rest, bucket)
        return verified

    def verify_items_count(self, master, bucket, num_attempt=3, timeout=2):
        # get the #of buckets from rest
        rest = RestConnection(master)
        replica_factor = bucket.replicaNumber
        vbucket_active_sum = 0
        vbucket_replica_sum = 0
        vbucket_pending_sum = 0
        all_server_stats = []
        stats_received = True
        nodes = rest.get_nodes()
        for server in nodes:
            # get the stats
            server_stats = BucketHelper(master).get_bucket_stats_for_node(
                bucket, server)
            if not server_stats:
                self.log.debug("Unable to get stats from {0}: {1}"
                               .format(server.ip, server.port))
                stats_received = False
            all_server_stats.append((server, server_stats))
        if not stats_received:
            raise StatsUnavailableException()
        sum = 0
        for server, single_stats in all_server_stats:
            if not single_stats or "curr_items" not in single_stats:
                continue
            sum += single_stats["curr_items"]
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
        self.log.debug(msg.format(sum, replica_factor + 1,
                                  (sum * (replica_factor + 1))))
        master_stats = BucketHelper(master).get_bucket_stats(bucket)
        if "curr_items_tot" in master_stats:
            self.log.debug('curr_items_tot from master: {0}'
                           .format(master_stats["curr_items_tot"]))
        else:
            raise Exception("Bucket {0} stats doesnt contain 'curr_items_tot':"
                            .format(bucket))
        if replica_factor >= len(nodes):
            self.log.warn("Number of nodes is less than replica requires")
            delta = sum * (len(nodes)) - master_stats["curr_items_tot"]
        else:
            delta = sum * (replica_factor + 1) - master_stats["curr_items_tot"]
        delta = abs(delta)

        if delta > 0:
            if sum == 0:
                missing_percentage = 0
            else:
                missing_percentage = delta * 1.0 / (sum * (replica_factor + 1))
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

    def _wait_warmup_completed(self, servers, bucket, wait_time=300):
        # Return True, if bucket_type is not equal to MEMBASE
        if bucket.bucketType != Bucket.Type.MEMBASE:
            return True

        self.log.debug("Waiting for bucket %s to complete warm-up"
                       % bucket.name)
        warmed_up = False
        start = time.time()
        for server in servers:
            # Cbstats implementation to wait for bucket warmup
            warmed_up = False
            shell = RemoteMachineShellConnection(server)
            cbstat_obj = Cbstats(shell)
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
            shell.disconnect()

        return warmed_up

    def add_rbac_user(self, testuser=None, rolelist=None, node=None):
        """
           From spock, couchbase server is built with some users that handles
           some specific task such as:
               cbadminbucket
           Default added user is cbadminbucket with admin role
        """
        if node is None:
            node = self.cluster.master
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
                       % (testuser[0]["name"], node.ip))
        RbacUtil().create_user_source(testuser, 'builtin', node)

        self.log.debug("**** Add '%s' role to '%s' user ****"
                       % (rolelist[0]["roles"], testuser[0]["name"]))
        status = RbacUtil().add_user_role(rolelist, RestConnection(node),
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

    def perform_verify_queries(self, num_views, prefix, ddoc_name, view_name,
                               query, wait_time=120, bucket="default",
                               expected_rows=None, retry_time=2, server=None):
        tasks = []
        result = True
        if server is None:
            server = self.cluster.master
        if expected_rows is None:
            expected_rows = self.num_items
        for i in xrange(num_views):
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

    def make_default_views(self, default_view, prefix, count,
                           is_dev_ddoc=False, different_map=False):
        ref_view = default_view
        ref_view.name = (prefix, ref_view.name)[prefix is None]
        if different_map:
            views = []
            for i in xrange(count):
                views.append(View(ref_view.name + str(i),
                                  'function (doc, meta) {'
                                  'emit(meta.id, "emitted_value%s");}'
                                  % str(i), None, is_dev_ddoc))
            return views
        else:
            return [
                View("{0}{1}".format(ref_view.name, i),
                     ref_view.map_func, None, is_dev_ddoc)
                for i in xrange(count)
            ]

    def base_bucket_ratio(self, servers):
        ratio = 1.0
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

    def set_flusher_total_batch_limit(self, node=None,
                                      flusher_total_batch_limit=3,
                                      buckets=None):
        self.log.debug("Changing the bucket properties by changing "
                       "flusher_total_batch_limit to {0}"
                       .format(flusher_total_batch_limit))
        if node is None:
            node = self.cluster.master
        rest = RestConnection(node)

        # Enable diag_eval outside localhost
        shell = RemoteMachineShellConnection(node)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        for bucket in buckets:
            code = "ns_bucket:update_bucket_props(\"" + bucket.name \
                   + "\", [{extra_config_string, " \
                   + "\"flusher_total_batch_limit=" \
                   + str(flusher_total_batch_limit) + "\"}])."
            rest.diag_eval(code)

        # Restart Memcached in all cluster nodes to reflect the settings
        for server in self.cluster_util.get_kv_nodes(master=node):
            shell = RemoteMachineShellConnection(server)
            shell.kill_memcached()
            shell.disconnect()

        # Check bucket-warm_up after Memcached restart
        retry_count = 10
        buckets_warmed_up = self.is_warmup_complete(buckets, retry_count)
        if not buckets_warmed_up:
            self.log.critical("Few bucket(s) not warmed up "
                              "within expected time")

        for server in self.cluster_util.get_kv_nodes(master=node):
            for bucket in buckets:
                mc = MemcachedClientHelper.direct_client(server, bucket)
                stats = mc.stats()
                self.assertTrue(int(stats['ep_flusher_total_batch_limit']) \
                                == flusher_total_batch_limit)

    def update_bucket_props(self, command, value, buckets=[], node=None):
        self.log.info("Changing the bucket properties by changing {0} to {1}".
                      format(command, value))
        if not buckets:
            buckets = self.buckets
        if node is None:
            node = self.cluster.master
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
        for server in self.cluster_util.get_kv_nodes(master=node):
            shell = RemoteMachineShellConnection(server)
            shell.restart_couchbase()
            shell.disconnect()

        # Check bucket-warm_up after Couchbase restart
        retry_count = 10
        buckets_warmed_up = self.is_warmup_complete(buckets, retry_count)
        if not buckets_warmed_up:
            self.log.critical("Few bucket(s) not warmed up "
                              "within expected time")

    def validate_manifest_uid(self, bucket):
        status = True
        manifest_uid = BucketHelper(self.cluster.master) \
            .get_bucket_manifest_uid(bucket)
        if bucket.stats.manifest_uid != int(manifest_uid, 16):
            BucketUtils.log.error("Bucket UID mismatch. "
                                  "Expected: %s, Actual: %s"
                                  % (bucket.stats.manifest_uid,
                                     manifest_uid))
            status = False
        return status

    def get_expected_total_num_items(self, bucket):
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

    def validate_doc_count_as_per_collections(self, bucket):
        """
        Function to validate doc_item_count as per the collection object's
        num_items value against cbstats count from KV nodes

        Throws exception if mismatch in stats.

        :param bucket: Bucket object using which the validation should be done
        """
        status = True
        collection_data = None
        cb_stat_objects = list()

        # Create required cb_stat objects
        for node in self.cluster_util.get_kv_nodes():
            shell = RemoteMachineShellConnection(node)
            cb_stat_objects.append(Cbstats(shell))

        for cb_stat in cb_stat_objects:
            tem_collection_data = cb_stat.get_collections(bucket)
            if collection_data is None:
                collection_data = tem_collection_data
            else:
                for key, value in tem_collection_data.items():
                    if type(value) is dict:
                        for col_name, c_data in value.items():
                            collection_data[key][col_name]['items'] \
                                += c_data['items']
        # Validate scope-collection hierarchy with doc_count
        status = \
            status \
            and self.validate_bucket_collection_hierarchy(bucket,
                                                          cb_stat_objects,
                                                          collection_data)

        # Disconnect all created shell connections
        for cb_stat in cb_stat_objects:
            cb_stat.shellConn.disconnect()

        # Raise exception if the status is 'False'
        if not status:
            raise Exception("Collections stat validation failed")

    def validate_docs_per_collections_all_buckets(self, timeout=300):
        self.log.info("Validating collection stats and item counts")
        vbucket_stats = self.get_vbucket_seqnos(
            self.cluster_util.get_kv_nodes(),
            self.buckets,
            skip_consistency=True)

        # Validate total expected doc_count matches with the overall bucket
        for bucket in self.buckets:
            status = self.validate_manifest_uid(bucket)
            if not status:
                self.log.warn("Bucket manifest UID mismatch!")

            expected_num_items = self.get_expected_total_num_items(bucket)
            self.verify_stats_for_bucket(bucket, expected_num_items,
                                         timeout=timeout)

            result = self.validate_seq_no_stats(vbucket_stats[bucket.name])
            self.assertTrue(result,
                            "snap_start and snap_end corruption found!!!")

            self.validate_doc_count_as_per_collections(bucket)

    def remove_scope_collections_for_bucket(self, bucket):
        """
        Delete all created scope-collection for the given bucket
        """
        for scope in self.get_active_scopes(bucket):
            for collection in self.get_active_collections(bucket, scope.name):
                self.drop_collection(self.cluster.master,
                                     bucket,
                                     scope_name=scope.name,
                                     collection_name=collection.name)
            if scope.name != CbServer.default_scope:
                self.drop_scope(self.cluster.master,
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

                    for collection_name in scope_data["collections"].keys():
                        collection_dict[collection_name] = dict()

        def perform_scope_operation(operation_type, ops_spec):
            buckets_spec = ops_spec
            if "buckets" in buckets_spec:
                buckets_spec = ops_spec["buckets"]
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
                            max_length=CbServer.max_scope_name_len)
                        ScopeUtils.create_scope(cluster.master,
                                                bucket,
                                                {"name": scope_name})
                        created_scope_collection_details[scope_name] = dict()
                        created_scope_collection_details[scope_name]["collections"] = dict()
                        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = dict()
                            with requests.Session() as session:
                                for _ in range(collection_count):
                                    collection_name = BucketUtils.get_random_name(
                                        max_length=CbServer.max_collection_name_len)
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
                            for scope_name in b_data["scopes"].keys():
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
                    for scope_name in b_data["scopes"].keys():
                        scope_obj = bucket.scopes[scope_name]
                        BucketUtils.create_scope(cluster.master,
                                                 bucket,
                                                 scope_obj.get_dict_object())
                        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = []
                            with requests.Session() as session:
                                for _, collection in scope_obj.collections.items():
                                    futures.append(executor.submit(ScopeUtils.create_collection, cluster.master,
                                                                   bucket, scope_name, collection.get_dict_object(),
                                                                   session))
                            for future in concurrent.futures.as_completed(futures):
                                if future.exception() is not None:
                                    raise future.exception()

        def perform_collection_operation(operation_type, ops_spec):
            if operation_type == "create":
                bucket_names = ops_spec.keys()
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
                            scope = sample(ops_spec[bucket_name]["scopes"].keys(),
                                           1)[0]
                            collection_name = BucketUtils.get_random_name(
                                max_length=CbServer.max_collection_name_len)
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
                                        scope_data["collections"].keys():
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
                                for col_name in scope_data["collections"].keys():
                                    collection = bucket.scopes[scope_name] \
                                        .collections[col_name]
                                    futures.append(executor.submit(ScopeUtils.create_collection, cluster.master,
                                                                   bucket, scope_name, collection.get_dict_object(),
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
                    CbServer.default_collection] = \
                    dict()
        cols_to_drop = BucketUtils.get_random_collections(
            buckets,
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_DROP, 0),
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, 1),
            exclude_from=exclude_from)

        # Start threads to drop collections
        c_flush_thread = threading.Thread(
            target=perform_collection_operation,
            args=["flush", cols_to_flush])
        c_drop_thread = threading.Thread(
            target=perform_collection_operation,
            args=["drop", cols_to_drop])

        c_flush_thread.start()
        c_drop_thread.start()

        c_flush_thread.join()
        c_drop_thread.join()

        # Fetch scopes to be dropped
        scopes_to_drop = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_TO_DROP, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, 1),
            avoid_default=True)
        scope_drop_thread = threading.Thread(
            target=perform_scope_operation,
            args=["drop", scopes_to_drop])

        scope_drop_thread.start()
        scope_drop_thread.join()

        # Fetch buckets under which scopes will be created
        create_scope_spec = dict()
        create_scope_spec["buckets"] = BucketUtils.get_random_buckets(
            buckets,
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"))
        create_scope_spec["scopes_num"] = \
            input_spec.get(MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET, 0)
        create_scope_spec["collection_count_under_each_scope"] = \
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES, 0)
        scope_create_thread = threading.Thread(
            target=perform_scope_operation,
            args=["create", create_scope_spec])

        scope_create_thread.start()
        scope_create_thread.join()

        # Fetch random Scopes under which to create collections
        scopes_to_create_collections = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, "all"),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"))
        scopes_to_create_collections["req_collections"] = \
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET, 0)
        collections_create_thread = threading.Thread(
            target=perform_collection_operation,
            args=["create", scopes_to_create_collections])

        collections_create_thread.start()
        collections_create_thread.join()

        # Fetch random scopes to recreate
        scopes_to_recreate = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_TO_RECREATE, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"),
            consider_only_dropped=True)
        scope_recreate_thread = threading.Thread(
            target=perform_scope_operation,
            args=["recreate", scopes_to_recreate])

        scope_recreate_thread.start()
        scope_recreate_thread.join()

        # Fetch random collections to recreate
        collections_to_recreate = BucketUtils.get_random_collections(
            buckets,
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_RECREATE, 0),
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, "all"),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"),
            consider_only_dropped=True)
        collection_recreate_thread = threading.Thread(
            target=perform_collection_operation,
            args=["recreate", collections_to_recreate])

        collection_recreate_thread.start()
        collection_recreate_thread.join()
        DocLoaderUtils.log.info("Done Performing scope/collection specific "
                                "operations")
        return ops_details

    @staticmethod
    def perform_tasks_from_spec_old(cluster, buckets, input_spec):
        """
        Perform Create/Drop/Flush of scopes and collections as specified
        in the input json spec template.
        (Old function that does CRUD on collections in sequential manner)
        ToDo:Remove this function after a few weekly runs

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

                    for collection_name in scope_data["collections"].keys():
                        collection_dict[collection_name] = dict()

        def perform_scope_operation(operation_type, ops_spec):
            buckets_spec = ops_spec
            if "buckets" in buckets_spec:
                buckets_spec = ops_spec["buckets"]

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
                    created_scope_collection_details = \
                        ScopeUtils.create_scopes(
                            cluster, bucket, scopes_num,
                            scope_name=None,
                            collection_count=collection_count)
                    ops_details["scopes_added"][bucket_name][
                        "scopes"].update(created_scope_collection_details)
                    ops_details["collections_added"][bucket_name][
                        "scopes"].update(created_scope_collection_details)
            elif operation_type == "drop":
                update_ops_details_dict("scopes_dropped", buckets_spec,
                                        fetch_collections=True)
                update_ops_details_dict("collections_dropped", buckets_spec,
                                        fetch_collections=True)
                for bucket_name, b_data in buckets_spec.items():
                    bucket = BucketUtils.get_bucket_obj(buckets,
                                                        bucket_name)
                    for scope_name in b_data["scopes"].keys():
                        ScopeUtils.drop_scope(cluster.master,
                                              bucket,
                                              scope_name)
            elif operation_type == "recreate":
                DocLoaderUtils.log.debug("Recreating scopes")
                update_ops_details_dict("scopes_added", buckets_spec)
                update_ops_details_dict("collections_added", buckets_spec,
                                        fetch_collections=True)
                for bucket_name, b_data in buckets_spec.items():
                    bucket = BucketUtils.get_bucket_obj(buckets,
                                                        bucket_name)
                    for scope_name in b_data["scopes"].keys():
                        scope_obj = bucket.scopes[scope_name]
                        BucketUtils.create_scope(cluster.master,
                                                 bucket,
                                                 scope_obj.get_dict_object())
                        for _, collection in scope_obj.collections.items():
                            ScopeUtils.create_collection(
                                cluster.master,
                                bucket,
                                scope_name,
                                collection.get_dict_object())

        def perform_collection_operation(operation_type, ops_spec):
            if operation_type == "create":
                created_collections = 0
                bucket_names = ops_spec.keys()
                bucket_names.remove("req_collections")
                while created_collections < ops_spec["req_collections"]:
                    bucket_name = sample(bucket_names, 1)[0]
                    bucket = BucketUtils.get_bucket_obj(buckets,
                                                        bucket_name)
                    scope = sample(ops_spec[bucket_name]["scopes"].keys(),
                                   1)[0]
                    created_names = CollectionUtils.create_collections(
                        cluster, bucket, 1, scope)
                    created_collections += 1
                    update_ops_details_dict(
                        "collections_added",
                        {bucket_name:
                            {"scopes":
                                {scope:
                                    {"collections": {
                                        created_names.keys()[0]: dict()
                                    }}}}})
            elif operation_type in ["flush", "drop"]:
                if operation_type == "drop":
                    update_ops_details_dict("collections_dropped", ops_spec)
                else:
                    update_ops_details_dict("collections_flushed", ops_spec)

                for bucket_name, b_data in ops_spec.items():
                    bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
                    for scope_name, scope_data in b_data["scopes"].items():
                        for collection_name in \
                                scope_data["collections"].keys():
                            if operation_type == "flush":
                                CollectionUtils.flush_collection(
                                    cluster.master, bucket,
                                    scope_name, collection_name)
                            elif operation_type == "drop":
                                CollectionUtils.drop_collection(
                                    cluster.master, bucket,
                                    scope_name, collection_name)
            elif operation_type == "recreate":
                DocLoaderUtils.log.debug("Recreating collections")
                update_ops_details_dict("collections_added", ops_spec)
                for bucket_name, b_data in ops_spec.items():
                    bucket = BucketUtils.get_bucket_obj(buckets, bucket_name)
                    for scope_name, scope_data in b_data["scopes"].items():
                        for col_name in scope_data["collections"].keys():
                            collection = bucket.scopes[scope_name] \
                                .collections[col_name]
                            ScopeUtils.create_collection(
                                cluster.master,
                                bucket,
                                scope_name,
                                collection.get_dict_object())

        DocLoaderUtils.log.info("Performing scope/collection specific "
                                "operations")

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
                    CbServer.default_collection] = \
                    dict()
        cols_to_drop = BucketUtils.get_random_collections(
            buckets,
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_DROP, 0),
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, 1),
            exclude_from=exclude_from)

        # Start threads to drop collections
        c_flush_thread = threading.Thread(
            target=perform_collection_operation,
            args=["flush", cols_to_flush])
        c_drop_thread = threading.Thread(
            target=perform_collection_operation,
            args=["drop", cols_to_drop])

        c_flush_thread.start()
        c_drop_thread.start()

        c_flush_thread.join()
        c_drop_thread.join()

        # Fetch scopes to be dropped
        scopes_to_drop = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_TO_DROP, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, 1),
            avoid_default=True)
        scope_drop_thread = threading.Thread(
            target=perform_scope_operation,
            args=["drop", scopes_to_drop])

        scope_drop_thread.start()
        scope_drop_thread.join()

        # Fetch buckets under which scopes will be created
        create_scope_spec = dict()
        create_scope_spec["buckets"] = BucketUtils.get_random_buckets(
            buckets,
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"))
        create_scope_spec["scopes_num"] = \
            input_spec.get(MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET, 0)
        create_scope_spec["collection_count_under_each_scope"] = \
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES, 0)
        scope_create_thread = threading.Thread(
            target=perform_scope_operation,
            args=["create", create_scope_spec])

        scope_create_thread.start()
        scope_create_thread.join()

        # Fetch random Scopes under which to create collections
        scopes_to_create_collections = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, "all"),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"))
        scopes_to_create_collections["req_collections"] = \
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET, 0)
        collections_create_thread = threading.Thread(
            target=perform_collection_operation,
            args=["create", scopes_to_create_collections])

        collections_create_thread.start()
        collections_create_thread.join()

        # Fetch random scopes to recreate
        scopes_to_recreate = BucketUtils.get_random_scopes(
            buckets,
            input_spec.get(MetaCrudParams.SCOPES_TO_RECREATE, 0),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"),
            consider_only_dropped=True)
        scope_recreate_thread = threading.Thread(
            target=perform_scope_operation,
            args=["recreate", scopes_to_recreate])

        scope_recreate_thread.start()
        scope_recreate_thread.join()

        # Fetch random collections to recreate
        collections_to_recreate = BucketUtils.get_random_collections(
            buckets,
            input_spec.get(MetaCrudParams.COLLECTIONS_TO_RECREATE, 0),
            input_spec.get(MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS, "all"),
            input_spec.get(MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS, "all"),
            consider_only_dropped=True)
        collection_recreate_thread = threading.Thread(
            target=perform_collection_operation,
            args=["recreate", collections_to_recreate])

        collection_recreate_thread.start()
        collection_recreate_thread.join()
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
