import json
import re
import string
import time
import urllib.parse
import uuid
import zlib
from concurrent.futures import TimeoutError as FutureTimeoutError
from random import choice, randint
from threading import Event, Thread

from couchbase.durability import PersistTo
from couchbase.options import IncrementOptions
from couchbase.exceptions import CouchbaseException, AmbiguousTimeoutException
from couchbase.logic.options import DeltaValueBase
from couchbase.transcoder import RawJSONTranscoder

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader

from SecurityLib.rbac import RbacUtil
from StatsLib.StatsOperations import StatsHelper
from basetestcase import ClusterSetup
from cb_constants import constants, CbServer, DocLoading
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.rest_client import RestConnection
from membase.api.rest_client import RestConnection as MembaseRestConnection
from cb_tools.cb_cli import CbCli
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from cb_tools.mc_stat import McStat
from cluster_utils.cluster_ready_functions import CBCluster
from constants.sdk_constants.java_client import SDKConstants
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from error_simulation.cb_error import CouchbaseError
from gsiLib.gsiHelper import GsiHelper

from mc_bin_client import MemcachedClient, MemcachedError
from memcacheConstants import ENGINE_PARAM_CHECKPOINT, ENGINE_PARAM_FLUSH, \
    ERR_ETMPFAIL
from memcached.helper.data_helper import MemcachedClientHelper, \
    VBucketAwareMemcached
from platform_constants.os_constants import Linux
from rebalance_utils.rebalance_util import RebalanceUtil
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
from sdk_utils.sdk_options import SDKOptions
from shell_util.remote_connection import RemoteMachineShellConnection
from table_view import TableView



"""
Capture basic get, set operations, also the meta operations.
This is based on some 4.1.1 test which had separate
bugs with incr and delete with meta and I didn't see an obvious home for them.

This is small now but we will reactively add things

These may be parameterized by:
   - full and value eviction
   - DGM and non-DGM
"""


class basic_ops(ClusterSetup):
    def setUp(self):
        super(basic_ops, self).setUp()

        if not self.skip_setup_cleanup:
            self.create_bucket(self.cluster)

        self.doc_ops = self.input.param("doc_ops", "").split(";")
        self.observe_test = self.input.param("observe_test", False)
        self.warmup_timeout = self.input.param("warmup_timeout", 300)
        # Scope/collection name can be default or create a random one to test
        self.scope_name = self.input.param("scope", CbServer.default_scope)
        self.collection_name = self.input.param("collection",
                                                CbServer.default_collection)

        # Create Scope/Collection with random names if not equal to default
        if self.scope_name != CbServer.default_scope:
            self.scope_name = self.bucket_util.get_random_name()
            self.bucket_util.create_scope(self.cluster.master,
                                          self.cluster.buckets[0],
                                          {"name": self.scope_name})
        if self.collection_name != CbServer.default_collection:
            self.collection_name = self.bucket_util.get_random_name()
            self.bucket_util.create_collection(self.cluster.master,
                                               self.cluster.buckets[0],
                                               self.scope_name,
                                               {"name": self.collection_name,
                                                "num_items": self.num_items})
            self.log.info("Using scope::collection - '%s::%s'"
                          % (self.scope_name, self.collection_name))

        # Update required num_items under default collection
        self.cluster.buckets[0] \
            .scopes[self.scope_name] \
            .collections[self.collection_name] \
            .num_items = self.num_items

        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            durability=self.durability_level,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to)

        # Create sdk_clients for pool
        if self.cluster.sdk_client_pool:
            self.log.info("Creating SDK client pool")
            self.cluster.sdk_client_pool.create_clients(
                self.cluster,
                self.cluster.buckets[0],
                req_clients=self.sdk_pool_capacity,
                compression_settings=self.sdk_compression)
        elif self.load_docs_using == "sirius_java_sdk":
            self.log.info("Creating SDK clients in Java side")
            for bucket in self.cluster.buckets:
                SiriusCouchbaseLoader.create_clients_in_pool(
                    self.cluster.master, self.cluster.master.rest_username,
                    self.cluster.master.rest_password,
                    bucket.name, req_clients=self.sdk_pool_capacity)

        # Set index storage to avoid failures during index creation
        rest = RestConnection(self.cluster.master)
        rest.activate_service_api(CbServer.Services.INDEX)
        rest.index.set_gsi_settings({"storageMode": "plasma"})

        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("==========Finished Basic_ops base setup========")

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def continuous_op_key_type(self):
        """
        Key generator class to use when the same doc range is first loaded
        and then driven by an 'async_continuous_doc_ops' task.

        Sirius runs continuous ops with iterations=-1, which forces its
        'CircularKey' generator. CircularKey derives from RandomKey, so its
        keys ('<rand>-<idx>') do not match the default SimpleKey layout
        ('<prefix><padding><idx>'). Loading with SimpleKey and then reading
        circularly silently addresses a different key namespace, so every
        read misses and server-side get counters never move. Load with
        RandomKey so both halves share one namespace.

        The in-built loader has no such split, so leave it untouched.
        """
        if self.load_docs_using == "sirius_java_sdk":
            return "RandomKey"
        return "SimpleKey"

    def do_basic_ops(self):
        key = 'key1'
        tombstone_key = 'key2'
        self.log.info('Starting basic ops')

        default_bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        sdk_client = SDKClient(self.cluster, default_bucket,
                               compression_settings=self.sdk_compression)
        cluster_rest = ClusterRestAPI(self.cluster.master)
        mcd_client = VBucketAwareMemcached(cluster_rest, default_bucket,
                                           info=self.cluster.master)
        mc_for_key = mcd_client.memcached(key)
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(self.cluster.master)
        cbepctl = Cbepctl(shell)

        # MB-17231 - incr with full eviction
        rc = sdk_client.collection._increment(
            key, IncrementOptions(delta=DeltaValueBase(1)))
        self.log.info('rc for incr: {0}'.format(rc))

        # MB-17289 del with meta
        rc = sdk_client.crud(DocLoading.Bucket.DocOps.UPDATE,
                             key, {'value': 'value2'},
                             persist_to=PersistTo.ONE)
        self.log.info('set is: {0}'.format(rc))

        self.sleep(5, "Wait before eviction")
        try:
            mc_for_key.evict_key(key)
        except MemcachedError as exp:
            self.fail("Exception with evict meta - {0}".format(exp))

        # MB-63769: Stop persistence + lookup tombstone
        self.log.info("Stopping persistence to avoid flusher run")
        cbepctl.persistence(default_bucket.name, "stop")
        # To create a key as a tombstone with custom CAS
        tombstone_cas = 0xabcd
        try:
            # key, exp, flags, seqno, cas
            mc_for_key.del_with_meta(tombstone_key, 0, 0, 2, tombstone_cas)
            sdk_client.crud(DocLoading.Bucket.SubDocOps.LOOKUP,
                            tombstone_key, "_sysXattr", xattr=True,
                            access_deleted=True)
        except Exception as exp:
            self.fail(f"Exception with del_with meta + lookup - {exp}")
        finally:
            cbepctl.persistence(default_bucket.name, "start")
            cbstats.disconnect()
            shell.disconnect()

    # Reproduce test case for MB-28078
    def do_setWithMeta_twice(self):
        key, value = "1", '{"Hello":"World"}'
        cluster_rest = ClusterRestAPI(self.cluster.master)
        mcd_client = VBucketAwareMemcached(
            cluster_rest, self.cluster.buckets[0],
            info=self.cluster.master)
        mc_for_key = mcd_client.memcached(key)
        try:
            mc_for_key.setWithMeta(key, value, 3600, 0, 1, 0x1512a3186faa0000,
                                   options=0)
        except MemcachedError as error:
            self.fail("Error on first setWithMeta :: "
                      f"<MemcachedError #{error.status} `{error.msg}`>")

        stats = mc_for_key.stats()
        self.log.info('curr_items: {0} and curr_temp_items:{1}'
                      .format(stats['curr_items'], stats['curr_temp_items']))
        self.sleep(5, "Wait before checking the stats")
        stats = mc_for_key.stats()
        self.log.info('curr_items: {0} and curr_temp_items:{1}'
                      .format(stats['curr_items'], stats['curr_temp_items']))

        try:
            mc_for_key.setWithMeta(key, value, 3600, 0, 1, 0x1512a3186faa0000,
                                   options=0)
        except MemcachedError as error:
            stats = mc_for_key.stats()
            self.log.info('After second setWithMeta(), '
                          f'curr_items: {stats["curr_items"]} '
                          f'and curr_temp_items: {stats["curr_temp_items"]}')
            self.assertEqual(int(stats['curr_temp_items']), 0,
                             "Error on 2nd setWithMeta(), "
                             "expected curr_temp_items == 0")
            self.log.info(f"<MemcachedError #{error.status} `{error.msg}`>")

    def generate_docs_bigdata(self, docs_per_day, start=0,
                              document_size=1024000,
                              vbuckets=CbServer.total_vbuckets):
        return doc_generator(self.key, start, docs_per_day,
                             key_size=self.key_size,
                             doc_size=document_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=vbuckets,
                             randomize_doc_size=self.randomize_doc_size,
                             randomize_value=self.randomize_value)

    def test_doc_size(self):
        def check_durability_failures():
            self.log.error(task.sdk_acked_curd_failed.keys())
            self.log.error(task.sdk_exception_crud_succeed.keys())
            self.assertTrue(
                len(task.sdk_acked_curd_failed) == 0,
                "Durability failed for docs: %s" % task.sdk_acked_curd_failed.keys())
            self.assertTrue(
                len(task.sdk_exception_crud_succeed) == 0,
                "Durability failed for docs: %s" % task.sdk_acked_curd_failed.keys())
        """
        Basic tests for document CRUD operations using JSON docs
        """
        doc_op = self.input.param("doc_op", None)
        doc_ops_loop = self.input.param("doc_ops_loop", 1)
        def_bucket = self.cluster.buckets[0]
        supported_d_levels = self.bucket_util.get_supported_durability_levels(
            minimum_level=SDKConstants.DurabilityLevel.MAJORITY)

        # Stat validation reference variables
        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        self.log.info("Creating doc_generator..")
        # Load basic docs into bucket
        doc_create = doc_generator(
            self.key, 0, self.num_items, key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type, target_vbucket=self.target_vbucket,
            vbuckets=def_bucket.numVBuckets,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value)
        self.log.info("Loading {0} docs into the bucket: {1}"
                      .format(self.num_items, def_bucket))
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_create,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            ryow=self.ryow,
            check_persistence=self.check_persistence,
            scope=self.scope_name,
            collection=self.collection_name,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)

        if self.ryow:
            check_durability_failures()

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        # Update ref_val
        verification_dict["ops_create"] += \
            self.num_items - len(task.fail.keys())
        # Validate vbucket stats
        if self.durability_level in supported_d_levels:
            verification_dict["sync_write_committed_count"] += self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(self.cluster),
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Verify initial doc load count
        self.log.info("Validating doc_count in buckets")
        self.bucket_util.validate_doc_count_as_per_collections(
            self.cluster, def_bucket)

        self.log.info("Creating doc_generator for doc_op")
        num_item_start_for_crud = int(self.num_items / 2)
        doc_update = doc_generator(
            self.key, 0, num_item_start_for_crud,
            key_size=self.key_size,
            doc_size=self.doc_size, doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=def_bucket.numVBuckets,
            mutate=1,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value)

        if self.target_vbucket:
            mutation_doc_count = len(doc_update.pre_generated_keys)
        else:
            mutation_doc_count = (doc_update.end - doc_update.start
                                  + len(task.fail.keys()))

        if doc_op == DocLoading.Bucket.DocOps.UPDATE:
            self.log.info("Performing 'update' mutation over the docs")
            task = self.task.async_load_gen_docs(
                self.cluster, def_bucket, doc_update,
                DocLoading.Bucket.DocOps.UPDATE, 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                ryow=self.ryow,
                check_persistence=self.check_persistence,
                scope=self.scope_name,
                collection=self.collection_name,
                iterations=doc_ops_loop,
                load_using=self.load_docs_using)
            if doc_ops_loop == -1:
                self.sleep(60, "Wait before killing the cont. update load")
                task.end_task()
            self.task.jython_task_manager.get_task_result(task)
            total_updates = num_item_start_for_crud
            verification_dict["ops_update"] += total_updates
            if self.durability_level in supported_d_levels:
                verification_dict["sync_write_committed_count"] \
                    += total_updates
            if self.ryow:
                check_durability_failures()

            # Read all the values to validate update operation
            task = self.task.async_validate_docs(
                    self.cluster, def_bucket,
                    doc_update, DocLoading.Bucket.DocOps.UPDATE, 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    scope=self.scope_name,
                    collection=self.collection_name,
                    validate_using=self.load_docs_using)
            self.assertTrue(
                self.task.jython_task_manager.get_task_result(task),
                "Validation task failed")

        elif doc_op == DocLoading.Bucket.DocOps.DELETE:
            self.log.info("Performing 'delete' mutation over the docs")
            task = self.task.async_load_gen_docs(
                self.cluster, def_bucket, doc_update,
                DocLoading.Bucket.DocOps.DELETE, 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                ryow=self.ryow, check_persistence=self.check_persistence,
                scope=self.scope_name,
                collection=self.collection_name,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)
            if self.collection_name is None:
                target_scope = CbServer.default_scope
                target_collection = CbServer.default_collection
            else:
                target_scope = self.scope_name
                target_collection = self.collection_name

            def_bucket \
                .scopes[target_scope] \
                .collections[target_collection] \
                .num_items -= (self.num_items - num_item_start_for_crud)
            verification_dict["ops_delete"] += mutation_doc_count

            if self.durability_level in supported_d_levels:
                verification_dict["sync_write_committed_count"] \
                    += mutation_doc_count
            if self.ryow:
                check_durability_failures()

            # Read all the values to validate delete operation
            task = self.task.async_validate_docs(
                self.cluster, def_bucket, doc_update,
                DocLoading.Bucket.DocOps.DELETE, 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                validate_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)

        elif doc_op is not None:
            self.log.warning("Unsupported doc_operation")

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(self.cluster),
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        self.log.info("Validating doc_count")
        self.bucket_util.validate_doc_count_as_per_collections(
            self.cluster, def_bucket)

    def test_large_doc_size(self):
        # bucket size=256MB, when Bucket gets filled 236MB then
        # test starts failing document size=2MB, No of docs = 221,
        # load 250 docs generate docs with size >= 1MB , See MB-29333

        self.doc_size *= 1024*1024
        for bucket in self.cluster.buckets:
            gens_load = self.generate_docs_bigdata(
                docs_per_day=self.num_items, document_size=self.doc_size,
                vbuckets=bucket.numVBuckets)
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gens_load,
                DocLoading.Bucket.DocOps.CREATE, 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)

        # check if all the documents(250) are loaded with default timeout
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

    def test_large_doc_20MB(self):
        # test reproducer for MB-29258,
        # Load a doc which is greater than 20MB
        # with compression enabled and check if it fails
        # check with compression_mode as active, passive and off

        for bucket in self.cluster.buckets:
            gens_load = self.generate_docs_bigdata(
                docs_per_day=1, document_size=(self.doc_size * 1024000),
                vbuckets=bucket.numVBuckets)
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gens_load,
                DocLoading.Bucket.DocOps.CREATE, 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)
            if self.doc_size > 20:
                if len(task.fail.keys()) == 0:
                    self.log_failure("No failures during large doc insert")
                for doc_id, doc_result in task.fail.items():
                    sdk_err = str(doc_result["error"])
                    if not self.bucket_util.check_if_exception_exists(
                            sdk_err, SDKException.ValueTooLargeException):
                        self.log_failure("Invalid exception for key %s: %s"
                                         % (doc_id, sdk_err))
            else:
                if len(task.fail.keys()) != 0:
                    self.log_failure("Failures during large doc insert")

        for bucket in self.cluster.buckets:
            if self.doc_size > 20:
                # failed with error "Data Too Big" when document size > 20MB
                self.bucket_util.verify_stats_all_buckets(self.cluster, 0)
            else:
                self.bucket_util.verify_stats_all_buckets(self.cluster, 1)
                gens_update = self.generate_docs_bigdata(
                    docs_per_day=1, document_size=(21 * 1024000),
                    vbuckets=bucket.numVBuckets)
                task = self.task.async_load_gen_docs(
                    self.cluster, bucket, gens_update,
                    DocLoading.Bucket.DocOps.UPDATE, 0,
                    batch_size=10,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    durability=self.durability_level,
                    compression=self.sdk_compression,
                    timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(task)
                if len(task.fail.keys()) != 1:
                    self.log_failure("Large docs inserted for keys: %s"
                                     % task.fail.keys())
                if len(task.fail.keys()) == 0:
                    self.log_failure("No failures during large doc insert")
                for key, crud_result in task.fail.items():
                    sdk_err = str(crud_result["error"])
                    if not self.bucket_util.check_if_exception_exists(
                            sdk_err, SDKException.ValueTooLargeException):
                        self.log_failure("Unexpected error for key %s: %s"
                                         % (key, sdk_err))
                for doc_id, doc_result in task.fail.items():
                    sdk_err = str(doc_result["error"])
                    if not self.bucket_util.check_if_exception_exists(
                            sdk_err, SDKException.ValueTooLargeException):
                        self.log_failure("Invalid exception for key %s: %s"
                                         % (doc_id, sdk_err))
                self.bucket_util.verify_stats_all_buckets(self.cluster, 1)
        self.validate_test_failure()

    def test_parallel_cruds(self):
        data_op_dict = dict()
        num_items = self.num_items
        half_of_num_items = self.num_items / 2
        supported_d_levels = self.bucket_util.get_supported_durability_levels()
        exp_values_to_test = [0, 3000, 10000, 12999]

        # Initial doc_loading
        initial_load = doc_generator(self.key, 0, self.num_items,
                                     doc_size=self.doc_size)
        task = self.task.async_load_gen_docs(
            self.cluster, self.cluster.buckets[0], initial_load,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=100, process_concurrency=8,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)

        # Create required doc_gens and doc_op task object
        for op_index, doc_op in enumerate(self.doc_ops):
            if doc_op == DocLoading.Bucket.DocOps.CREATE:
                num_items += half_of_num_items
                gen_start = self.num_items
                gen_end = self.num_items + half_of_num_items
            elif doc_op == DocLoading.Bucket.DocOps.DELETE:
                gen_start = 0
                gen_end = half_of_num_items
            else:
                gen_start = half_of_num_items
                gen_end = self.num_items

            d_level = ""
            replicate_to = persist_to = 0
            if self.observe_test:
                if self.num_replicas > 0:
                    replicate_to = randint(1, self.num_replicas)
                    persist_to = randint(0, self.num_replicas + 1)
            else:
                d_level = choice(supported_d_levels)

            doc_ttl = choice(exp_values_to_test)
            self.log.info("Doc_op %s, range (%d, %d), ttl=%s, "
                          "replicate_to=%s, persist_to=%s, d_level=%s"
                          % (doc_op, gen_start, gen_end, doc_ttl,
                             replicate_to, persist_to, d_level))

            # Required to handle similar doc_ops like create,create case
            dict_key = "%s_%s" % (doc_op, op_index)
            data_op_dict[dict_key] = dict()
            data_op_dict[dict_key]["doc_gen"] = doc_generator(
                self.key, gen_start, gen_end,
                doc_size=self.doc_size,
                mutation_type=doc_op)
            data_op_dict[dict_key]["task"] = self.task.async_load_gen_docs(
                self.cluster, self.cluster.buckets[0],
                data_op_dict[dict_key]["doc_gen"], doc_op,
                exp=doc_ttl,
                compression=self.sdk_compression,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=d_level, timeout_secs=self.sdk_timeout,
                process_concurrency=1, batch_size=1,
                print_ops_rate=False, start_task=False,
                task_identifier="%s_%d" % (doc_op, op_index),
                load_using=self.load_docs_using)

        # Start all tasks
        for op_index, doc_op in enumerate(self.doc_ops):
            dict_key = "%s_%s" % (doc_op, op_index)
            self.task_manager.add_new_task(data_op_dict[dict_key]["task"])
        # Wait for doc_ops to complete and validate final doc value result
        for op_index, doc_op in enumerate(self.doc_ops):
            dict_key = "%s_%s" % (doc_op, op_index)
            self.task_manager.get_task_result(data_op_dict[dict_key]["task"])
            self.log.info("%s task completed" % doc_op)
            if data_op_dict[dict_key]["task"].fail:
                self.log_failure("Doc_loading failed for %s: %s"
                                 % (doc_op,
                                    data_op_dict[dict_key]["task"].fail))
            elif doc_op in [DocLoading.Bucket.DocOps.CREATE,
                            DocLoading.Bucket.DocOps.UPDATE,
                            DocLoading.Bucket.DocOps.REPLACE,
                            DocLoading.Bucket.DocOps.DELETE]:
                # Docs could have expired during CRUD, will get KEY_ENOENT
                if data_op_dict[dict_key]["task"].exp == exp_values_to_test[1]:
                    continue
                suppress_err_tbl = False
                if doc_op == DocLoading.Bucket.DocOps.DELETE:
                    suppress_err_tbl = True
                self.log.info("Validating %s results" % doc_op)
                # Read all the values to validate doc_operation values
                task = self.task.async_validate_docs(
                    self.cluster, self.cluster.buckets[0],
                    data_op_dict[dict_key]["doc_gen"], doc_op, 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    suppress_error_table=suppress_err_tbl,
                    validate_using=self.load_docs_using)
                self.task.jython_task_manager.get_task_result(task)

        self.validate_test_failure()

    def test_diag_eval_curl(self):
        # Check if diag/eval can be done only by local host
        self.disable_diag_eval_on_non_local_host = \
            self.input.param("disable_diag_eval_non_local", False)
        port = self.cluster.master.port

        # check if local host can work fine
        cmd = list()
        cmd_base = 'curl http://{0}:{1}@localhost:{2}/diag/eval ' \
            .format(self.cluster.master.rest_username,
                    self.cluster.master.rest_password, port)
        command = cmd_base + '-X POST -d \'os:cmd("env")\''
        cmd.append(command)
        command = cmd_base + '-X POST ' \
                             '-d \'case file:read_file("/etc/passwd") ' \
                             'of {ok, B} -> io:format("~p~n", ' \
                             '[binary_to_term(B)]) end.\''
        cmd.append(command)

        shell = RemoteMachineShellConnection(self.cluster.master)
        for command in cmd:
            output, error = shell.execute_command(command)
            self.assertNotEquals("API is accessible from localhost only",
                                 output[0])

        # Disable allow_nonlocal_eval
        if not self.disable_diag_eval_on_non_local_host:
            command = cmd_base + '-X POST -d \'ns_config:set(' \
                                 'allow_nonlocal_eval, true).\''
            _, _ = shell.execute_command(command)

        # Check ip address on diag/eval will not work fine
        # when allow_nonlocal_eval is disabled
        cmd = []
        cmd_base = 'curl http://{0}:{1}@{2}:{3}/diag/eval ' \
            .format(self.cluster.master.rest_username,
                    self.cluster.master.rest_password,
                    self.cluster.master.ip, port)
        command = cmd_base + '-X POST -d \'os:cmd("env")\''
        cmd.append(command)
        command = cmd_base + '-X POST ' \
                             '-d \'case file:read_file("/etc/passwd") ' \
                             'of {ok, B} -> io:format("~p~n", ' \
                             '[binary_to_term(B)]) end.\''
        cmd.append(command)

        for command in cmd:
            output, error = shell.execute_command(command)
            if self.disable_diag_eval_on_non_local_host:
                self.assertEquals("API is accessible from localhost only",
                                  output[0])
            else:
                self.assertNotEquals("API is accessible from localhost only",
                                     output[0])

    def test_bucket_ops_with_bucket_reader_user(self):
        uname = "bucket_reader"
        user = [{'id': uname, 'name': uname, 'password': 'password'}]
        role_list = [{'id': uname, 'name': uname, 'roles': 'data_writer[*]'}]
        key = "test_doc_1"
        val = {"f": "v"}

        self.log.info("Creating user %s" % uname)
        self.bucket_util.add_rbac_user(self.cluster.master,
                                       testuser=user, rolelist=role_list)
        client = SDKClient(self.cluster, self.cluster.buckets[0],
                           username=uname, password="password")

        try:
            self.log.info("Perform regular update")
            result = client.crud(DocLoading.Bucket.DocOps.UPDATE, key, val)
            self.assertTrue(result["status"], "Update op failed")
            result = client.crud(DocLoading.Bucket.DocOps.UPDATE, key, val,
                                 durability="NONE",
                                 replicate_to=0, persist_to=0)
            self.assertTrue(result["status"], "Update op failed")

            self.log.info("Performing update with observe")
            result = client.crud(
                DocLoading.Bucket.DocOps.UPDATE, key, val,
                durability="NONE", replicate_to=1, persist_to=2)
            self.assertFalse(result["status"], "Observe operation succeeded")

            self.log.info("Performing read op")
            result = client.crud(DocLoading.Bucket.DocOps.READ, key)
            self.assertFalse(result["status"], "Read op succeeded")
            self.assertTrue(
                SDKException.AuthenticationException in result["error"],
                "Invalid exception type")
            self.assertTrue("EACCESS" in result["error"],
                            "Expected error string not found")
        finally:
            client.close()

    def test_MB_40967(self):
        """
        1. Load initial docs into the bucket
        2. Perform continuous reads until get_cmd stats breaks in
           'cbstats timings' command
        """
        total_gets = 0
        max_gets = 2500000000
        # Soak window (in seconds) for the read-loop below. 'max_gets' is
        # not reachable within any sane CI budget, so this bound is what
        # actually ends a healthy run
        test_timeout = self.input.param("test_timeout", 1800)
        # Consecutive checks tolerated with 'ops_get' flat at zero before
        # treating the reads as stuck. Only a sustained zero counts: this
        # test resets memcached stats in parallel, so the counter is
        # expected to jump around and even go backwards on a healthy run
        max_stagnant_checks = self.input.param("max_stagnant_checks", 5)
        bucket = self.cluster.buckets[0]
        doc_gen = doc_generator(self.key, 0, self.num_items,
                                doc_size=0,
                                load_using=self.load_docs_using,
                                key_type=self.continuous_op_key_type())
        create_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=100, process_concurrency=self.process_concurrency,
            timeout_secs=self.sdk_timeout, load_using=self.load_docs_using)
        self.task_manager.get_task_result(create_task)

        cbstat = dict()
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        for node in kv_nodes:
            cbstat[node] = Cbstats(node)

        self.log.info("Start doc_reads until total_gets cross: %s" % max_gets)
        read_task = self.task.async_continuous_doc_ops(
            self.cluster, bucket, doc_gen,
            op_type=DocLoading.Bucket.DocOps.READ, batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.sleep(60, "Wait for read task to start")
        stop_time = time.time() + test_timeout
        stagnant_checks = 0

        while total_gets < max_gets:
            total_gets = 0
            for node in kv_nodes:
                output, error = cbstat[node].get_timings(bucket.name)
                if error:
                    self.log_failure("Error during cbstat timings: %s" % error)
                    break

                get_cmd_found = False
                for line in output:
                    if "get_cmd_" in line:
                        if "get_cmd_mean" in line:
                            break
                        get_cmd_found = True
                if not get_cmd_found:
                    self.log.error(output)
                    self.log_failure("cbstat timings get_cmd stats not found")
                    break
                vb_details = cbstat[node].vbucket_details(bucket.name)
                for _, vb_stats in vb_details.items():
                    total_gets += int(vb_stats["ops_get"])
            if self.test_failure:
                break

            if total_gets > 0:
                stagnant_checks = 0
            else:
                stagnant_checks += 1
                if stagnant_checks >= max_stagnant_checks:
                    self.log_failure(
                        "ops_get stuck at 0 across %s consecutive checks. "
                        "Reads are not reaching the server"
                        % stagnant_checks)
                    break

            if time.time() >= stop_time:
                # Soak window elapsed without hitting the condition under
                # test. That is the expected outcome for this regression
                # test, so stop cleanly instead of running until the CI
                # job's own timeout kills the build
                self.log.info("Soak window of %s seconds elapsed. "
                              "total_gets=%s" % (test_timeout, total_gets))
                break

            # 'itr_count' exists only on the default_loader's task object
            itr_count = getattr(read_task, "itr_count", "NA")
            self.sleep(120, "Total_gets: %s, itr: %s" % (total_gets,
                                                         itr_count))

        for node in kv_nodes:
            cbstat[node].disconnect()

        read_task.end_task()
        self.task_manager.get_task_result(read_task)

        self.validate_test_failure()

    def test_MB_41510(self):
        """
        1. Load initial docs into the bucket
        2. Perform continuous reads
        3. Perform 'mcstat reset' in parallel to the reads
        4. Perform 'cbstats timings' command to read the current values
        5. Validate there is no crash when stats are getting reset continuously
        """

        def reset_mcstat(bucket_name):
            mc_stat = dict()
            for t_node in kv_nodes:
                shell_conn = RemoteMachineShellConnection(t_node)
                mc_stat[t_node] = McStat(shell_conn)

            while not stop_thread:
                for t_node in list(mc_stat.keys()):
                    try:
                        mc_stat[t_node].reset(bucket_name)
                    except Exception as mcstat_err:
                        self.log_failure(mcstat_err)
                if self.test_failure:
                    break

        def get_timings(bucket_name):
            cb_stat = dict()
            for t_node in kv_nodes:
                cb_stat[t_node] = Cbstats(t_node)

            while not stop_thread:
                for t_node in list(cb_stat.keys()):
                    try:
                        cb_stat[t_node].get_timings(bucket_name)
                    except Exception as cbstat_err:
                        self.log_failure(cbstat_err)
                if self.test_failure:
                    break

            for t_node in kv_nodes:
                cb_stat[t_node].disconnect()

        total_gets = 0
        max_gets = 50000000
        stop_thread = False
        # Soak window (in seconds) for the read-loop below. 'max_gets' is
        # not reachable within any sane CI budget, so this bound is what
        # actually ends a healthy run
        test_timeout = self.input.param("test_timeout", 1800)
        # Consecutive checks tolerated with 'ops_get' flat at zero before
        # treating the reads as stuck. Only a sustained zero counts: this
        # test resets memcached stats in parallel, so the counter is
        # expected to jump around and even go backwards on a healthy run
        max_stagnant_checks = self.input.param("max_stagnant_checks", 5)
        bucket = self.cluster.buckets[0]
        cb_stat_obj = dict()
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        for node in kv_nodes:
            cb_stat_obj[node] = Cbstats(node)

        doc_gen = doc_generator(self.key, 0, self.num_items, doc_size=1,
                                load_using=self.load_docs_using,
                                key_type=self.continuous_op_key_type())
        create_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=500, process_concurrency=self.process_concurrency,
            timeout_secs=self.sdk_timeout, load_using=self.load_docs_using)
        self.task_manager.get_task_result(create_task)

        mc_stat_reset_thread = Thread(target=reset_mcstat, args=[bucket.name])
        get_timings_thread = Thread(target=get_timings, args=[bucket.name])
        mc_stat_reset_thread.start()
        get_timings_thread.start()

        read_task = self.task.async_continuous_doc_ops(
            self.cluster, bucket, doc_gen,
            op_type=DocLoading.Bucket.DocOps.READ,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)

        stop_time = time.time() + test_timeout
        stagnant_checks = 0

        while total_gets < max_gets:
            total_gets = 0
            try:
                for node in list(cb_stat_obj.keys()):
                    vb_details = cb_stat_obj[node].vbucket_details(bucket.name)
                    for _, vb_stats in vb_details.items():
                        total_gets += int(vb_stats["ops_get"])
            except Exception as err:
                self.log_failure(err)

            self.log.info("Total gets: %s" % total_gets)
            result = self.check_coredump_exist(self.servers,
                                               force_collect=True)

            if result is True:
                self.log_failure("Cb_logs validation failed")
                break
            elif self.test_failure:
                break

            if total_gets > 0:
                stagnant_checks = 0
            else:
                stagnant_checks += 1
                if stagnant_checks >= max_stagnant_checks:
                    self.log_failure(
                        "ops_get stuck at 0 across %s consecutive checks. "
                        "Reads are not reaching the server"
                        % stagnant_checks)
                    break

            if time.time() >= stop_time:
                # Soak window elapsed without hitting the condition under
                # test. That is the expected outcome for this regression
                # test, so stop cleanly instead of running until the CI
                # job's own timeout kills the build
                self.log.info("Soak window of %s seconds elapsed. "
                              "total_gets=%s" % (test_timeout, total_gets))
                break

            self.sleep(60, "Wait before next check")

        stop_thread = True
        read_task.end_task()

        for node in kv_nodes:
            cb_stat_obj[node].disconnect()

        mc_stat_reset_thread.join()
        get_timings_thread.join()

        self.validate_test_failure()

    def test_MB_41255(self):
        def create_docs_with_xattr():
            value = {'val': 'a' * self.doc_size}
            xattr_kv = ["field", "value"]
            while not stop_loader:
                t_key = "%s-%s" % (self.key, self.num_items)
                crud_result = client.crud(DocLoading.Bucket.DocOps.CREATE,
                                          t_key, value, timeout=60)
                if crud_result["status"] is False:
                    self.log_failure("Create key %s failed: %s"
                                     % (t_key, crud_result["error"]))
                    break
                self.num_items += 1
                client.crud("subdoc_insert", t_key, xattr_kv, xattr=True)

        nodes_data = dict()
        stop_loader = False
        non_resident_keys = list()
        non_resident_keys_len = 0
        self.num_items = 0
        max_keys_to_del = 250
        self.active_resident_threshold = \
            int(self.input.param("active_resident_threshold", 99))
        bucket = self.cluster.buckets[0]

        for node in self.cluster_util.get_kv_nodes(self.cluster):
            nodes_data[node] = dict()
            nodes_data[node]["cbstats"] = Cbstats(node)
            nodes_data[node]["active_vbs"] = nodes_data[node][
                "cbstats"].vbucket_list(bucket.name, "active")
            nodes_data[node]["replica_vbs"] = nodes_data[node][
                "cbstats"].vbucket_list(bucket.name, "replica")

        bucket_helper = BucketHelper(self.cluster.master)
        client = SDKClient(self.cluster, bucket)

        self.log.info("Loading documents until %s%% DGM is achieved"
                      % self.active_resident_threshold)
        dgm_thread = Thread(target=create_docs_with_xattr)
        dgm_thread.start()

        # Run doc_loading until the targeted DGM value is hit
        while not stop_loader:
            dgm_value = bucket_helper.fetch_bucket_stats(bucket.name)["op"][
                "samples"]["vb_active_resident_items_ratio"][-1]
            if dgm_value <= self.active_resident_threshold:
                self.log.info("DGM value: %s" % dgm_value)
                stop_loader = True

        dgm_thread.join()
        self.log.info("Loaded %s documents" % self.num_items)

        # Wait for ep_engine_queue size to become '0'
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        # Fetch evicted keys
        self.log.info("Fetching keys evicted from replica vbs")
        for doc_index in range(self.num_items):
            key = "%s-%s" % (self.key, doc_index)
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(
                key, bucket.numVBuckets)
            for node, n_data in nodes_data.items():
                if vb_for_key in n_data["replica_vbs"]:
                    stat = n_data["cbstats"].vkey_stat(bucket.name, key,
                                                       vbucket_num=vb_for_key)
                    if stat["is_resident"] == "false":
                        non_resident_keys.append(key)
                        non_resident_keys_len += 1
                    break

            if non_resident_keys_len >= max_keys_to_del:
                break

        for _, n_data in nodes_data.items():
            n_data["cbstats"].disconnect()

        self.log.info("Non-resident key count: %d" % non_resident_keys_len)

        # Start rebalance-out operation
        rebalance_out = self.task.async_rebalance(
            self.cluster, [], [self.cluster.servers[-1]])
        self.sleep(10, "Wait for rebalance to start")

        # Start deleting the evicted docs in parallel to rebalance task
        self.log.info("Deleting evicted keys")
        for key in non_resident_keys:
            result = client.crud(DocLoading.Bucket.DocOps.DELETE, key)
            if result["status"] is False:
                self.log_failure("Key %s deletion failed: %s"
                                 % (key, result["error"]))

        # Wait for rebalance to complete
        self.task_manager.get_task_result(rebalance_out)

        # Wait for ep_engine_queue size to become '0'
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        # Trigger compaction
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)

        # Read all deleted keys (include replica read) to validate
        for key in non_resident_keys:
            result = client.get_from_all_replicas(key)
            if result:
                self.log_failure("Key '%s' exists on %d replica(s)"
                                 % (key, len(result)))

        # Close SDK and shell connections
        client.close()

        self.assertTrue(rebalance_out.result, "Rebalance_out failed")

        self.bucket_util.verify_stats_all_buckets(
            self.cluster,
            self.num_items - non_resident_keys_len)
        self.validate_test_failure()

    def test_MB_41405(self):
        """
        1. Pick random vbucket number
        2. Create, Delete doc_keys and validate on_disk_deleted counter moves
        3. Fetch bloom_filter_size during first delete op and run_compaction
        4. Create-delete 10K more items and run compaction again
        5. Make sure current bloom_filter_size is > the value during step#3
        """
        def validate_crud_result(op_type, doc_key, crud_result):
            if crud_result["status"] is False:
                self.log_failure("Key %s %s failed: %s"
                                 % (doc_key, op_type, crud_result["error"]))

        on_disk_deletes = 0
        bloom_filter_size = None
        bucket = self.cluster.buckets[0]
        target_vb = choice(range(bucket.numVBuckets))
        vb_str = str(target_vb)
        doc_gen = doc_generator(self.key, 0, self.num_items,
                                vbuckets=bucket.numVBuckets,
                                target_vbucket=[target_vb])

        target_node = None
        nodes_data = dict()
        for node in self.cluster_util.get_kv_nodes(self.cluster):
            nodes_data[node] = dict()
            nodes_data[node]["cbstats"] = Cbstats(node)
            nodes_data[node]["active_vbs"] = nodes_data[node][
                "cbstats"].vbucket_list(bucket.name, "active")
            if target_vb in nodes_data[node]["active_vbs"]:
                target_node = node

        # Open SDK client for doc_ops
        client = SDKClient(self.cluster, bucket)

        self.log.info("Testing using vbucket %s" % target_vb)
        while doc_gen.has_next():
            key, val = doc_gen.next()
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(
                key, bucket.numVBuckets)

            # Create and delete a key
            result = client.crud(DocLoading.Bucket.DocOps.CREATE, key, val)
            validate_crud_result(DocLoading.Bucket.DocOps.CREATE, key, result)
            result = client.crud(DocLoading.Bucket.DocOps.DELETE, key, val)
            validate_crud_result(DocLoading.Bucket.DocOps.DELETE, key, result)
            on_disk_deletes += 1

            # Wait for ep_queue_size to become zero
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)

            dcp_vb_takeover_stats = nodes_data[target_node][
                "cbstats"].dcp_vbtakeover(bucket.name, vb_for_key, key)
            if dcp_vb_takeover_stats["on_disk_deletes"] != on_disk_deletes:
                self.log_failure("Stat on_disk_deleted mismatch. "
                                 "Actual :: %s, Expected :: %s"
                                 % (dcp_vb_takeover_stats["on_disk_deletes"],
                                    on_disk_deletes))

            # Record bloom filter and perform compaction for the first item
            if bloom_filter_size is None:
                vb_details_stats = nodes_data[target_node][
                    "cbstats"].vbucket_details(bucket.name)
                bloom_filter_size = \
                    vb_details_stats[vb_str]["bloom_filter_size"]
                self.log.info("Bloom filter size before compaction: %s"
                              % bloom_filter_size)

                self.bucket_util._run_compaction(self.cluster,
                                                 number_of_times=1)

                vb_details_stats = nodes_data[target_node][
                    "cbstats"].vbucket_details(bucket.name)
                bloom_filter_size_after_compaction = \
                    vb_details_stats[vb_str]["bloom_filter_size"]
                self.log.info("Bloom filter size after compaction: %s"
                              % bloom_filter_size_after_compaction)

        # Create and delete 10K more items to validate bloom_filter_size
        doc_gen = doc_generator(self.key, self.num_items, self.num_items+10000,
                                vbuckets=bucket.numVBuckets,
                                target_vbucket=[target_vb])
        self.log.info("Loading 10K items for bloom_filter_size validation")
        while doc_gen.has_next():
            key, val = doc_gen.next()
            # Create and delete a key
            client.crud(DocLoading.Bucket.DocOps.CREATE, key, val)
            client.crud(DocLoading.Bucket.DocOps.DELETE, key, val)
            # self.bucket_util._wait_for_stats_all_buckets()

        self.bucket_util._run_compaction(self.cluster, number_of_times=1)
        self.sleep(5, "Compaction complete")
        vb_details_stats = nodes_data[target_node][
            "cbstats"].vbucket_details(bucket.name)
        bloom_filter_size_after_compaction = \
            vb_details_stats[vb_str]["bloom_filter_size"]
        self.log.info("Bloom filter size after compaction: %s"
                      % bloom_filter_size_after_compaction)
        if int(bloom_filter_size_after_compaction) <= int(bloom_filter_size):
            self.log_failure("Bloom filter init_size <= curr_size")

        for _, n_data in nodes_data.items():
            n_data["cbstats"].disconnect()

        # Close SDK and shell connections
        client.close()

        self.validate_test_failure()

    def test_MB_43055(self):
        """
        1. Load till low_wm
        2. Make non_io_threads=0
        3. Load few more docs and so that we do exceed the high_wm,
           this schedules the item pager
        4. Delete few docs to go below low_wm
        5. Make non_io_threads=default. Now the item pager tries to run,
           but finds mem_used < low_wat so exits without paging anything,
           triggering the bug
        6. Load docs to cross high_wm
        7. Confirm that the item pager never runs successfully,
           even though the memory usage is back above the high watermark
        """
        def perform_doc_op(op_type):
            start = self.num_items
            if op_type == DocLoading.Bucket.DocOps.DELETE:
                start = self.del_items
            doc_gen = doc_generator(self.key, start, start + load_batch,
                                    doc_size=self.doc_size,
                                    randomize_value=True)
            doc_op_task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_gen, op_type,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False, skip_read_on_error=True,
                suppress_error_table=True,
                batch_size=1, process_concurrency=1,
                load_using=self.load_docs_using)
            try:
                self.task_manager.get_task_result(doc_op_task,
                                                  timeout=doc_op_timeout)
            except FutureTimeoutError:
                self.fail(
                    "%s op did not complete within %ss - load task "
                    "appears stalled" % (op_type, doc_op_timeout))
            # The watermark loops below only observe 'mem_used'. A rejected
            # write leaves it flat, so without this the loops just spin
            # until their timeout with no clue why
            if getattr(doc_op_task, "fail", None):
                sample = list(doc_op_task.fail.items())[:3]
                self.fail("%s op failed for %s of %s docs. Sample: %s"
                          % (op_type, len(doc_op_task.fail), load_batch,
                             sample))
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            if op_type == DocLoading.Bucket.DocOps.CREATE:
                self.num_items += load_batch
            elif op_type == DocLoading.Bucket.DocOps.DELETE:
                self.del_items += load_batch

        def actual_nonio_thread_count(t_node):
            """
            Real size of the NonIO executor pool, which is what decides
            whether the ItemPager can run. This is not the same thing as the
            configured 'num_nonio_threads' value, and 'cbstats all' does not
            carry it - all_stats() cannot even parse the 'ep_workload:<key>'
            lines, so read 'cbstats workload' directly
            """
            try:
                output, _ = nodes_data[t_node]["cbstat"].get_stats(
                    bucket.name, "workload", field_to_grep="num_nonio")
                if isinstance(output, str):
                    output = output.split("\n")
                for line in output:
                    if "num_nonio" in line:
                        return line.split(":")[-1].strip()
            except Exception as err:
                self.log.warning("Could not read NonIO pool size: %s" % err)
            return "n/a"

        def display_bucket_water_mark_values(t_node):
            wm_tbl.rows = list()
            a_stats = nodes_data[t_node]["cbstat"].all_stats(bucket.name)
            wm_tbl.add_row(["High water_mark", a_stats["ep_mem_high_wat"],
                            a_stats["ep_mem_high_wat_percent"]])
            wm_tbl.add_row(["Low water_mark", a_stats["ep_mem_low_wat"],
                            a_stats["ep_mem_low_wat_percent"]])
            wm_tbl.add_row(["Num pager runs", a_stats["ep_num_pager_runs"],
                            ""])
            wm_tbl.add_row(["Memory Used", a_stats["mem_used"],
                            ""])
            wm_tbl.display("Memory stats")
            return a_stats

        stats = None
        nodes_data = dict()
        self.num_items = 0
        self.del_items = 0
        load_batch = 20
        doc_op_timeout = self.input.param("doc_op_timeout", 120)
        # Failsafe timeout (secs) to avoid hanging forever waiting for a
        # watermark to be reached (job getting aborted instead of failing)
        wm_wait_timeout = self.input.param("wm_wait_timeout", 600)
        # To provide little 'headroom' while loading/deleting docs in batches
        mem_buffer_gap = 10000
        low_wm_reached = False
        high_wm_reached = False
        # 'ep_num_pager_runs' at the moment num_nonio_threads was set to 0
        pager_runs_at_freeze = 0
        wm_tbl = TableView(self.log.info)
        bucket = self.cluster.buckets[0]

        wm_tbl.set_headers(["Stat", "Memory Val", "Percent"])
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)

        for node in kv_nodes:
            shell = RemoteMachineShellConnection(node)
            nodes_data[node] = dict()
            nodes_data[node]["shell"] = shell
            nodes_data[node]["cbstat"] = Cbstats(node)
            nodes_data[node]["eviction_start"] = False
            nodes_data[node]["active_vbs"] = nodes_data[node][
                "cbstat"].vbucket_list(bucket.name, "active")
            nodes_data[node]["replica_vbs"] = nodes_data[node][
                "cbstat"].vbucket_list(bucket.name, "replica")

        target_node = choice(kv_nodes)
        cbepctl = Cbepctl(nodes_data[target_node]["shell"])

        self.log.info("Loading till low_water_mark is reached")
        loop_start_time = time.time()
        while not low_wm_reached:
            if time.time() - loop_start_time > wm_wait_timeout:
                self.fail(
                    "Failed to reach low_water_mark within %ss "
                    "(loaded %s docs so far)"
                    % (wm_wait_timeout, self.num_items))
            perform_doc_op(DocLoading.Bucket.DocOps.CREATE)
            stats = nodes_data[target_node]["cbstat"].all_stats(bucket.name)
            if int(stats["mem_used"]) > int(stats["ep_mem_low_wat"]):
                display_bucket_water_mark_values(target_node)
                self.log.info("Low water_mark reached")
                low_wm_reached = True

                if int(stats["ep_num_pager_runs"]) != 0:
                    self.log_failure("ItemPager has run while loading")
                else:
                    self.log.info("Setting num_nonio_threads=0")
                    cbepctl.set(bucket.name,
                                "flush_param", "num_nonio_threads", 0)
                    pager_runs_at_freeze = int(stats["ep_num_pager_runs"])
                    nonio_threads = actual_nonio_thread_count(target_node)
                    self.log.info("NonIO pool size after the change: %s"
                                  % nonio_threads)

        load_batch = 1
        self.log.info("Loading docs till high_water_mark is reached")
        loop_start_time = time.time()
        while not high_wm_reached:
            if time.time() - loop_start_time > wm_wait_timeout:
                self.fail(
                    "Failed to reach high_water_mark within %ss "
                    "(loaded %s docs so far, mem_used=%s, high_wm=%s)"
                    % (wm_wait_timeout, self.num_items,
                       stats["mem_used"], stats["ep_mem_high_wat"]))
            perform_doc_op(DocLoading.Bucket.DocOps.CREATE)
            stats = nodes_data[target_node]["cbstat"].all_stats(bucket.name)
            self.log.debug("docs=%s mem_used=%s high_wm=%s pager_runs=%s"
                           % (self.num_items, stats["mem_used"],
                              stats["ep_mem_high_wat"],
                              stats["ep_num_pager_runs"]))
            if int(stats["ep_num_pager_runs"]) != pager_runs_at_freeze:
                # Steps 3-7 all assume the ItemPager cannot run while
                # num_nonio_threads=0. If it runs anyway it evicts memory
                # back down, high_wm is never crossed, and the loop above
                # just burns its timeout. Note that on kv_engine
                # ThreadPoolConfig::NonIoThreadCount::Default == 0, i.e. a
                # configured 0 can mean "auto-size the NonIO pool" rather
                # than "no NonIO threads" - in which case this test has no
                # way to set up its precondition and needs a supported
                # mechanism to freeze the pager
                self.fail(
                    "ItemPager kept running after num_nonio_threads=0: "
                    "ep_num_pager_runs %s -> %s after %s docs "
                    "(mem_used=%s, low_wm=%s, high_wm=%s). NonIO pool "
                    "still has %s thread(s), so the pager is not frozen and "
                    "steps 3-7 of this test cannot be exercised"
                    % (pager_runs_at_freeze, stats["ep_num_pager_runs"],
                       self.num_items, stats["mem_used"],
                       stats["ep_mem_low_wat"], stats["ep_mem_high_wat"],
                       actual_nonio_thread_count(target_node)))
            if int(stats["mem_used"]) > int(stats["ep_mem_high_wat"]):
                display_bucket_water_mark_values(target_node)
                self.log.info("High water_mark reached")
                high_wm_reached = True

        if not high_wm_reached:
            self.log_failure("Failed to reach high_wm with the given load")

        if int(stats["ep_num_pager_runs"]) != 0:
            self.log_failure("ItemPager has run with non_io_threads=0")

        self.log.info("Delete docs until the mem_used goes below low_wm")
        low_wm_reached = False
        while not low_wm_reached and self.del_items < self.num_items:
            perform_doc_op(DocLoading.Bucket.DocOps.DELETE)
            stats = nodes_data[target_node]["cbstat"].all_stats(bucket.name)
            if int(stats["mem_used"]) < (int(stats["ep_mem_low_wat"])
                                         - mem_buffer_gap):
                low_wm_reached = True
                display_bucket_water_mark_values(target_node)
                self.log.info("Low water_mark reached")

        if int(stats["ep_num_pager_runs"]) != 0:
            self.log_failure("ItemPager ran after del_op & non_io_threads=0")

        self.log.info("Setting num_nonio_threads=8")
        cbepctl.set(bucket.name, "flush_param", "num_nonio_threads", 8)

        self.sleep(10, "Wait after setting num_nonio_threads=8")
        stats = display_bucket_water_mark_values(target_node)
        if int(stats["ep_num_pager_runs"]) != 0:
            self.log_failure("ItemPager run with lower_wm levels")

        self.log.info("Loading docs till high_water_mark is reached")
        high_wm_reached = False
        loop_start_time = time.time()
        while not high_wm_reached:
            if time.time() - loop_start_time > wm_wait_timeout:
                self.fail(
                    "Failed to reach high_water_mark within %ss "
                    "(loaded %s docs so far)"
                    % (wm_wait_timeout, self.num_items))
            perform_doc_op(DocLoading.Bucket.DocOps.CREATE)
            stats = nodes_data[target_node]["cbstat"].all_stats(bucket.name)
            if int(stats["mem_used"]) > (int(stats["ep_mem_high_wat"])
                                         + mem_buffer_gap):
                high_wm_reached = True
                self.log.info("High water_mark reached")
                retry_count = 0
                while retry_count < 5:
                    retry_count += 1
                    stats = display_bucket_water_mark_values(target_node)
                    if int(stats["ep_num_pager_runs"]) > 1:
                        break
                    self.sleep(1, "ep_num_pager_runs=%s, expected > 1"
                                  % stats["ep_num_pager_runs"])
                else:
                    self.log_failure("ItemPager not triggered with high_wm")

            elif int(stats["ep_num_pager_runs"]) > 5:
                high_wm_reached = True
                self.log.info("ep_num_pager_runs started running")

        # Closing all shell connections
        for node in list(nodes_data.keys()):
            nodes_data[node]["shell"].disconnect()
            nodes_data[node]["cbstat"].disconnect()

        self.validate_test_failure()

    def test_MB_42918(self):
        """
        - Add item for some key
        - Stop persistence
        - Delete item
        - Do durable write with PersistMajority for same key
        - Doc get should return KEY_NOENT
        """

        doc_val = {"field": "val"}
        bucket = self.cluster.buckets[0]
        shell = RemoteMachineShellConnection(self.cluster.master)
        cb_err = CouchbaseError(self.log,
                                shell,
                                node=self.cluster.master)

        client_1 = SDKClient(self.cluster, bucket)
        client_2 = SDKClient(self.cluster, bucket)

        # Perform create-delete to populate bloom-filter
        client_1.crud(DocLoading.Bucket.DocOps.CREATE, self.key, doc_val)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        client_1.crud(DocLoading.Bucket.DocOps.DELETE, self.key)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, 0)

        # Create the document using async-write
        client_1.crud(DocLoading.Bucket.DocOps.CREATE, self.key, doc_val)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, 1)

        # Stop persistence and delete te document
        cb_err.create(CouchbaseError.STOP_PERSISTENCE, bucket.name)
        self.sleep(2, "Wait after stop_persistence")
        client_1.crud(DocLoading.Bucket.DocOps.DELETE, self.key)

        # Get doc to make sure we see not_found exception
        result = client_1.crud(DocLoading.Bucket.DocOps.READ, self.key)
        if not self.bucket_util.check_if_exception_exists(
                result["error"], SDKException.DocumentNotFoundException):
            self.log.info("Result: %s" % result)
            self.log_failure("Invalid exception with deleted_doc: %s"
                             % result["error"])

        # Perform sync-write to create doc prepare in hash-table
        create_thread = Thread(
            target=client_1.crud,
            args=[DocLoading.Bucket.DocOps.CREATE, self.key, doc_val],
            kwargs={"durability": SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY,
                    "timeout": 15})
        create_thread.start()
        self.sleep(5, "Wait to make sure prepare is generated")

        # Doc read should return not_found
        result = client_2.crud(DocLoading.Bucket.DocOps.READ, self.key)
        if not self.bucket_util.check_if_exception_exists(
                result["error"], SDKException.DocumentNotFoundException):
            self.log.info("Result: %s" % result)
            self.log_failure("Invalid exception with prepared doc: %s"
                             % result["error"])
        result = client_2.get_from_all_replicas(self.key)
        if result:
            self.log_failure("Able to read deleted value: %s" % result)
        create_thread.join()

        cb_err.revert(CouchbaseError.STOP_MEMCACHED, bucket.name)

        # Close shell and SDK connections
        client_1.close()
        client_2.close()
        shell.disconnect()

        self.validate_test_failure()

    def test_mb_47267(self):
        """
               1. Create a single KV node
               2. Create two buckets A and B
               3. Add a large number of documents to all vbucket in bucket A
               4. Add few documents to each vbucket in bucket B (orders of
               magnitude less)
               5. Shutdown and warmup the node (with node A warming up first)
               6. Verify that we're able to access vbucket state of each
               vbucket for bucket B before bucket A is fully warmed up.
                   With the idea that the warmup of bucket B isn't blocked by
                   the warmup for bucket A despite bucket A having a large
                   number of documents.
        """
        shell_conn = dict()
        error_sim = dict()
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_helper.update_memcached_settings(
            num_writer_threads="default",
            num_storage_threads="default",
            num_reader_threads="default"
        )
        self.bucket_util.create_default_bucket(
            self.cluster,
            ram_quota=100,
            replica=0,
            eviction_policy=self.bucket_eviction_policy,
            bucket_name="small_bucket"
        )

        big_bucket = self.cluster.buckets[0]
        small_bucket = self.cluster.buckets[1]

        # Big bucket docs generation
        doc_gen = doc_generator(self.key, 0, self.num_items, doc_size=100,
                                load_using=self.load_docs_using)
        load_task = self.task.async_load_gen_docs(
            self.cluster, big_bucket, doc_gen,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=500,
            process_concurrency=8,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            print_ops_rate=False,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(load_task)

        # Small bucket docs generation
        doc_gen_small = doc_generator(self.key, 0, 500, doc_size=100,
                                      load_using=self.load_docs_using)
        load_task_2 = self.task.async_load_gen_docs(
            self.cluster, small_bucket, doc_gen_small,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=500,
            process_concurrency=8,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            print_ops_rate=False,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(load_task_2)

        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        # thread manage
        target_nodes = choice(self.cluster_util.get_kv_nodes(self.cluster))
        bucket_helper.update_memcached_settings(
            num_reader_threads=1,
        )

        # Create shell_connections
        shell_conn[target_nodes.ip] = RemoteMachineShellConnection(
            target_nodes)
        # Perform specified action
        error_sim[target_nodes.ip] = CouchbaseError(self.log,
                                                    shell_conn[target_nodes.ip],
                                                    node=target_nodes)
        error_sim[target_nodes.ip].create(CouchbaseError.KILL_MEMCACHED,
                                          bucket_name=big_bucket.name)
        self.assertTrue(
            self.bucket_util._wait_warmup_completed(small_bucket,
                                                    [target_nodes])
            and (not self.bucket_util._wait_warmup_completed(
                 big_bucket, [target_nodes], self.warmup_timeout)),
            "Bucket with less data not accessible "
            "when other bucket getting warmed up.")
        # Disconnecting shell_connections
        shell_conn[target_nodes.ip].disconnect()

    def test_MB_41942(self):
        """
        1. Load huge dataset into bucket with replica=1
        2. Set doc_ttl for few docs on active node with persistence stopped
        3. Kill memcached during loading
        4. Set expiry pager to run during warmup
        5. Kill memcached again such that kill happens before warmup completes
        6. Validate high_seqno and uuid
        """
        bucket = self.cluster.buckets[0]
        target_node = choice(self.cluster_util.get_kv_nodes(self.cluster))
        self.log.info("Target node %s" % target_node.ip)
        shell = RemoteMachineShellConnection(target_node)
        cb_stat = Cbstats(target_node)
        cb_error = CouchbaseError(self.log,
                                  shell,
                                  node=target_node)

        # Load initial data set into bucket
        self.log.info("Loading %s docs into bucket" % self.num_items)
        doc_gen = doc_generator(self.key, 0, self.num_items,
                                doc_size=10000)
        load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=500, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            print_ops_rate=False,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(load_task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        self.durability_level = SDKConstants.DurabilityLevel.MAJORITY
        active_vbs = cb_stat.vbucket_list(bucket.name,
                                          vbucket_type="active")
        doc_gen = doc_generator(self.key, 0, 10000,
                                doc_size=100, vbuckets=bucket.numVBuckets,
                                target_vbucket=active_vbs,
                                load_using=self.load_docs_using)

        # Load with doc_ttl set
        self.log.info("Setting doc_ttl=1 for %s docs" % 10000)
        load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen,
            DocLoading.Bucket.DocOps.UPDATE, exp=1,
            batch_size=2000, process_concurrency=5,
            durability=self.durability_level,
            timeout_secs=30,
            skip_read_on_error=True,
            print_ops_rate=False,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(load_task)

        # Read task to trigger expiry_purger
        load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen,
            DocLoading.Bucket.DocOps.READ,
            batch_size=500, process_concurrency=8,
            timeout_secs=30,
            suppress_error_table=True,
            start_task=False,
            print_ops_rate=False,
            load_using=self.load_docs_using)

        # Baseline is collected *before* any kill. 'cbstats vbucket-details'
        # over 1024 vbuckets takes a couple of seconds over SSH, and doing
        # that between "warmup started" and the second kill was enough for
        # warmup to finish, which made step 5 unreachable
        before_stats = None
        while before_stats is None:
            before_stats = cb_stat.vbucket_details(bucket.name)

        retry = 0
        read_task_started = False
        killed_during_warmup = False
        max_retries = self.input.param("mc_kill_retries", 20)
        # Kill memcached during ttl load
        cb_error.create(CouchbaseError.KILL_MEMCACHED)
        while not killed_during_warmup and retry < max_retries:
            try:
                warmup_stats = cb_stat.warmup_stats(bucket.name)
                self.log.info("Current warmup state %s:%s"
                              % (warmup_stats["ep_warmup_thread"],
                                 warmup_stats["ep_warmup_state"]))
                if warmup_stats["ep_warmup_thread"] != "complete":
                    if not read_task_started:
                        self.log.info("Starting read task to trigger purger")
                        self.task_manager.add_new_task(load_task)
                        read_task_started = True
                    # Re-read as late as possible: this is the state the
                    # kill below actually lands on
                    warmup_stats = cb_stat.warmup_stats(bucket.name)
                    cb_error.create(CouchbaseError.KILL_MEMCACHED)
                    self.log.info("Warmup state during mc_kill %s:%s"
                                  % (warmup_stats["ep_warmup_thread"],
                                     warmup_stats["ep_warmup_state"]))
                    # Warmup can finish in the gap between the two reads. That
                    # is a missed window, not a product issue - the kill above
                    # restarts warmup, so loop round and try to land inside it
                    if warmup_stats["ep_warmup_thread"] != "complete":
                        killed_during_warmup = True
            except Exception:
                pass
            finally:
                retry += 1
                self.sleep(0.3)

        if read_task_started:
            self.task_manager.get_task_result(load_task)
        if not killed_during_warmup:
            self.log_failure("Can't trust the outcome, could not kill "
                             "memcached while the bucket was warming up "
                             "within %s attempts" % max_retries)
        while True:
            try:
                after_stats = cb_stat.vbucket_details(bucket.name)
                break
            except Exception:
                pass

        self.log.info("Validating high_seqno/uuid from vbucket-details")
        for vb_num, stats in before_stats.items():
            t_stat = "high_seqno"
            pre_kill_stat = before_stats[vb_num]
            post_kill_stat = after_stats[vb_num]
            if int(pre_kill_stat[t_stat]) > int(post_kill_stat[t_stat]):
                self.log_failure("%s::%s - %s > %s"
                                 % (vb_num, t_stat,
                                    pre_kill_stat[t_stat],
                                    post_kill_stat[t_stat]))
            t_stat = "uuid"
            if vb_num in active_vbs \
                    and pre_kill_stat[t_stat] == post_kill_stat[t_stat]:
                self.log_failure("%s %s: %s == %s"
                                 % (vb_num, t_stat,
                                    pre_kill_stat[t_stat],
                                    post_kill_stat[t_stat]))

        cb_stat.disconnect()
        shell.disconnect()
        self.validate_test_failure()

    def test_store_value_del_updates_datatype(self):
        """
        1. Bucket with replica=1
        2. Create document with system_xattr set
        3. Delete the document on active (system_xattr is still accessible)
        4. Load bucket into DGM to trigger the deleted doc eviction
        5. Rebalance_in a new node such that the vb with the deleted doc
           becomes as replica vb (Still holding the doc in the HT)
        6. Remove the sys_xattr from the deleted doc and rebalance_out the
           new node so the replica switches back to active vb
        7. Now the dbdump the current replica node should display the
           data_type=raw instead of 'xattr'
        8. Create a new cluster (single node) for XDCR with default bucket
        9. Start replication from the source to the xdcr bucket
           and expect no crash till the xdcr replication is complete.
        Note: Crash was seen in the ticket due to the fact that the source
              bucket was trying to read the document in memory with type
              xttr instead of raw during the xdcr replication process
        Ref: MB-52793
        """
        key, val = "test_key", {"f": "value"}
        sub_doc = ["_key", "value"]
        bucket = self.cluster.buckets[0]
        key_vb = self.bucket_util.get_vbucket_num_for_key(
            key, bucket.numVBuckets)
        in_node = self.cluster.servers[1]
        num_items = 0

        client = SDKClient(self.cluster, bucket)

        self.log.info("Creating tombstone '%s' with sys-xattr" % key)
        # Create a document
        client.crud(DocLoading.Bucket.DocOps.CREATE, key, val)
        # Load sys-xattr for the document
        client.crud(DocLoading.Bucket.SubDocOps.INSERT,
                    key, sub_doc, xattr=True)
        # Wait for ep_queue_size to become Zero
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])
        # Delete the document
        client.crud(DocLoading.Bucket.DocOps.DELETE, key)
        # Wait for ep_queue_size to become Zero
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])
        client.crud(DocLoading.Bucket.SubDocOps.LOOKUP, key, sub_doc[0],
                    xattr=True, access_deleted=True)

        self.log.info("Loading docs to make the tombstone doc as non-resident")
        is_resident = True
        start_index = 0
        batch_size = 1000
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstat = Cbstats(self.cluster.master)

        hash_dump_cmd = \
            "%s -u %s -p %s localhost:%d raw \"_hash-dump %d\" | grep %s" \
            % (Linux.COUCHBASE_BIN_PATH + "cbstats",
               self.cluster.master.rest_username,
               self.cluster.master.rest_password,
               self.cluster.master.memcached_port, key_vb, key)

        # Loading docs until the target doc is evicted from memory
        while is_resident:
            doc_gen = doc_generator("docs", start_index, batch_size,
                                    key_size=100, doc_size=10240,
                                    vbuckets=bucket.numVBuckets,
                                    target_vbucket=[key_vb])
            while doc_gen.has_next():
                d_key, val = doc_gen.next()
                client.crud(DocLoading.Bucket.DocOps.CREATE, d_key, val)

            output, _ = shell.execute_command(hash_dump_cmd)
            if not output:
                is_resident = False
            start_index = doc_gen.itr
            num_items += batch_size

        # Close the shell connections
        shell.disconnect()

        result = self.task.rebalance(self.cluster, [in_node], [])
        self.assertTrue(result, "Rebalance_in failed")

        replica_vbs = cbstat.vbucket_list(bucket.name, Bucket.vBucket.REPLICA)
        cbstat.disconnect()
        if key_vb not in replica_vbs:
            # Swap the nodes in-order to maintain the vbucket consistency
            in_node = self.cluster.servers[0]
            self.cluster.master = self.cluster.servers[1]

            cbstat = Cbstats(self.cluster.servers[1])
            replica_vbs = cbstat.vbucket_list(bucket.name,
                                              Bucket.vBucket.REPLICA)
            cbstat.disconnect()

        self.assertTrue(key_vb in replica_vbs, "vBucket is still active vb")

        client.crud(DocLoading.Bucket.SubDocOps.REMOVE, key, sub_doc[0],
                    xattr=True, access_deleted=True)
        client.close()

        # Rebalance out the new node
        result = self.task.rebalance(self.cluster,
                                     to_add=[], to_remove=[in_node])
        self.assertTrue(result, "Rebalance_out failed")

        self.log.info("Starting XDCR replication")
        xdcr_cluster = CBCluster("C2", servers=[in_node])
        xdcr_cluster.nodes_in_cluster = [in_node]
        xdcr_rest = MembaseRestConnection(xdcr_cluster.master)
        xdcr_rest.init_node()
        xdcr_rest.set_internalSetting("magmaMinMemoryQuota", 256)
        self.create_bucket(xdcr_cluster)
        rest = MembaseRestConnection(self.cluster.master)
        rest.add_remote_cluster(xdcr_cluster.master.ip,
                                xdcr_cluster.master.port,
                                xdcr_cluster.master.rest_username,
                                xdcr_cluster.master.rest_password,
                                xdcr_cluster.master.ip)
        rest.start_replication("continuous",
                               self.cluster.buckets[0].name,
                               xdcr_cluster.master.ip,
                               toBucket=xdcr_cluster.buckets[0].name)
        try:
            self.log.info("Waiting for all items to get replicated")
            self.bucket_util.verify_stats_all_buckets(xdcr_cluster,
                                                      num_items, timeout=180)
            # MB-55446 validation
            remote_uuid = xdcr_rest.get_pools_default()["controllers"][
                "replication"]["createURI"].split("=")[-1]
            q_str = urllib.parse.quote_plus(
                "/{}/{}/{}/meta_latency_wt"
                .format(remote_uuid, self.cluster.buckets[0].name,
                        xdcr_cluster.buckets[0].name))
            api = "{}/pools/default/buckets/{}/stats/replications{}" \
                .format(rest.baseUrl, self.cluster.buckets[0], q_str)
            status, content, _ = rest._http_request(
                api, 'GET', headers=rest._create_headers())
            self.log.info("{}::{}".format(status, content))
        finally:
            self.log.info("Removing xdcr bucket and remote references")
            self.bucket_util.delete_all_buckets(xdcr_cluster)
            rest.remove_all_replications()
            rest.remove_all_remote_clusters()

    def test_stats_with_warmup(self):
        """
        Ref: MB-53829
        """
        bucket = self.cluster.buckets[0]
        doc_create = doc_generator(
            self.key, 0, self.num_items, key_size=self.key_size,
            doc_size=self.doc_size, doc_type=self.doc_type)
        self.log.info("Loading {0} docs into the bucket: {1}"
                      .format(self.num_items, bucket))
        task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_create,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=self.batch_size,
            process_concurrency=10,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=self.scope_name, collection=self.collection_name,
            print_ops_rate=False)
        self.task.jython_task_manager.get_task_result(task)
        task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_create,
            DocLoading.Bucket.DocOps.UPDATE, 0,
            batch_size=self.batch_size,
            process_concurrency=10,
            durability=SDKConstants.DurabilityLevel.MAJORITY,
            timeout_secs=self.sdk_timeout,
            scope=self.scope_name, collection=self.collection_name,
            print_ops_rate=False,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)

        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        shell = RemoteMachineShellConnection(self.cluster.master)
        cb_err = CouchbaseError(self.log,
                                shell,
                                node=self.cluster.master)
        cb_stats = Cbstats(self.cluster.master)

        self.log.info("Collection stats before executing the scenario")
        cb_err.create(CouchbaseError.STOP_SERVER)
        cb_err.revert(CouchbaseError.STOP_SERVER)
        self.cluster_util.wait_for_ns_servers_or_assert([self.cluster.master])

        curr_stats = cb_stats.all_stats(bucket.name)
        for field in ["ep_db_file_size", "ep_db_data_size"]:
            self.assertTrue(int(curr_stats[field]) != 0,
                            "%s stat is zero" % field)

        cb_err.create(CouchbaseError.KILL_MEMCACHED)
        self.sleep(10, "Wait for memcached to recover")
        curr_stats = cb_stats.all_stats(bucket.name)
        for field in ["ep_db_file_size", "ep_db_data_size"]:
            self.assertTrue(int(curr_stats[field]) != 0,
                            "%s stat is zero" % field)
        cb_stats.disconnect()
        shell.disconnect()

    def test_warmup_scan_reset(self):
        """
        1. Create couchstore value eviction bucket
        2. Disable compaction
        3. Load limited number of docs on each vbucket (async then sync write)
        4. Restart memcached and validate the the num_items per each vb
        5. Also check warmedUpValues and warmedUpKeys from warmup stats

        Ref: MB-53415
        :return:
        """
        docs_per_vb = 2
        doc_val = {"f": "test"}
        bucket = self.cluster.buckets[0]
        total_docs = bucket.numVBuckets * docs_per_vb
        doc_keys = dict([(vb_num, list())
                        for vb_num in range(0, bucket.numVBuckets)])
        index = -1
        req_key_for_vb = list(doc_keys.keys())
        d_level = SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE

        param = "warmup_backfill_scan_chunk_duration"
        param_val = 0

        # Create required doc_keys for each vbucket
        self.log.info("Creating document keys for loading")
        while req_key_for_vb:
            index += 1
            key = "%s_%s" % (self.key, index)
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(
                key, bucket.numVBuckets)
            if vb_for_key in req_key_for_vb:
                doc_keys[vb_for_key].append(key)
                if len(doc_keys[vb_for_key]) == docs_per_vb:
                    req_key_for_vb.remove(vb_for_key)

        # Open SDK client for loading
        client = SDKClient(self.cluster, bucket)

        # Load doc with async writes
        self.log.info("Loading documents to each vbucket")
        for vb_num in list(doc_keys.keys()):
            # Async write
            key = doc_keys[vb_num][0]
            result = client.crud(DocLoading.Bucket.DocOps.CREATE, key, doc_val)
            self.assertTrue(result["status"], "Key '%s' insert failed" % key)

            # Sync write
            key = doc_keys[vb_num][1]
            result = client.crud(DocLoading.Bucket.DocOps.CREATE, key, doc_val,
                                 durability=d_level)
            self.assertTrue(result["status"], "Key '%s' insert failed" % key)

        # Close the SDK client
        client.close()

        # Wait for ep_queue_size to become Zero
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        # Setting warmup_backfill_scan_chunk_duration=0 using diag_eval
        diag_eval_cmd_format = \
            'curl -u %s:%s -X POST localhost:8091/diag/eval' \
            ' -d "ns_bucket:update_bucket_props(\\"%s\\",' \
            ' [{extra_config_string, \\"%s=%s\\"}])."' \

        diag_eval_cmd = diag_eval_cmd_format \
            % (self.cluster.master.rest_username,
               self.cluster.master.rest_password,
               bucket.name, param, param_val)

        cbstat = Cbstats(self.cluster.master)
        shell = RemoteMachineShellConnection(self.cluster.master)
        cb_err = CouchbaseError(self.log,
                                shell,
                                node=self.cluster.master)

        self.log.info("Running diag_eval and restarting couchbase-server")
        shell.execute_command(diag_eval_cmd)
        self.sleep(5, "Wait before restarting the cluster")
        shell.restart_couchbase()

        # Wait until bucket completes warmup
        self.log.info("Wait for warmup to complete")
        self.bucket_util.is_warmup_complete(self.cluster.buckets)

        # Validate the diag_eval command is reflected in the server
        all_stats = cbstat.all_stats(bucket.name)
        curr_val = int(all_stats["ep_warmup_backfill_scan_chunk_duration"])
        self.assertEqual(curr_val, param_val,
                         "Unexpected value: '%s'" % curr_val)

        # Run memcached restart and validate the warmup and vb stats
        for index in range(1, 10):
            self.log.info("Running iteration: %s" % index)
            cb_err.create(CouchbaseError.KILL_MEMCACHED)

            self.log.info("Wait for warmup to complete")
            self.bucket_util.is_warmup_complete(self.cluster.buckets)

            warmup_stat_keys = ["ep_warmup_estimated_key_count",
                                "ep_warmup_key_count",
                                "ep_warmup_value_count"]
            # 'ep_warmup_estimated_value_count' is only populated when
            # warmup goes through the access-log phase. The access scanner
            # has not run on a freshly created bucket, so the stat stays
            # 'unknown' - validate it only when the server reports a number
            optional_stat_keys = ["ep_warmup_estimated_value_count"]
            # Warmup stats can briefly report 'unknown' right after
            # is_warmup_complete() returns, so poll until they settle
            # into numeric values instead of failing with a ValueError.
            warmup_stats = cbstat.all_stats(bucket.name, "warmup")
            retry_count = 0
            while retry_count < 10 and any(
                    not warmup_stats[key].isdigit()
                    for key in warmup_stat_keys):
                retry_count += 1
                self.sleep(1, "Waiting for warmup stats to settle")
                warmup_stats = cbstat.all_stats(bucket.name, "warmup")

            for key in warmup_stat_keys:
                self.assertTrue(warmup_stats[key].isdigit(),
                                "%s never settled to a numeric value: %s"
                                % (key, warmup_stats[key]))
                self.assertFalse(int(warmup_stats[key]) != total_docs,
                                 "Value mismatch. %s = %s"
                                 % (key, warmup_stats[key]))
            for key in optional_stat_keys:
                if not warmup_stats[key].isdigit():
                    self.log.info("%s not reported by the server: %s"
                                  % (key, warmup_stats[key]))
                    continue
                self.assertFalse(int(warmup_stats[key]) != total_docs,
                                 "Value mismatch. %s = %s"
                                 % (key, warmup_stats[key]))

            vb_details = cbstat.vbucket_details(bucket.name)
            for vb_num, vb_stats in vb_details.items():
                self.assertFalse(int(vb_stats["num_items"]) != docs_per_vb,
                                 "Vb %s reports less num_items: %s"
                                 % (vb_num, vb_stats["num_items"]))

        param_val = 100
        self.log.info("Reset the value back to %s" % param_val)
        diag_eval_cmd = diag_eval_cmd_format \
            % (self.cluster.master.rest_username,
               self.cluster.master.rest_password,
               bucket.name, param, param_val)

        self.log.info("Running diag_eval and restarting couchbase-server")
        self.log.info(diag_eval_cmd)
        shell.execute_command(diag_eval_cmd)
        self.sleep(5, "Wait before restarting the cluster")
        shell.restart_couchbase()

        self.log.info("Wait for warmup to complete")
        self.bucket_util.is_warmup_complete(self.cluster.buckets)

        all_stats = cbstat.all_stats(bucket.name)
        curr_val = int(all_stats["ep_warmup_backfill_scan_chunk_duration"])
        self.assertEqual(curr_val, param_val,
                         "Unexpected value: '%s'" % curr_val)

        cbstat.disconnect()
        shell.disconnect()

    def verify_stat(self, items, value="active"):
        mc = MemcachedClient(self.cluster.master.ip,
                             constants.memcached_port)
        mc.sasl_auth_plain(self.cluster.master.rest_username,
                           self.cluster.master.rest_password)
        mc.bucket_select('default')
        stats = mc.stats()
        self.assertEquals(stats['ep_compression_mode'], value)
        self.assertEquals(int(stats['ep_item_compressor_num_compressed']),
                          items)
        self.assertNotEquals(int(stats['vb_active_itm_memory']),
                             int(stats['vb_active_itm_memory_uncompressed']))

    def test_compression_active_and_off(self):
        """
        test reproducer for MB-29272,
        Load some documents with compression mode set to active
        get the cbstats
        change compression mode to off and wait for minimum 250ms
        Load some more documents and check the compression is not done
        epengine.basic_ops.basic_ops.test_compression_active_and_off,items=10000,compression_mode=active

        :return:
        """
        # Load some documents with compression mode as active
        gen_create = doc_generator("eviction1_",
                                   start=0,
                                   end=self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   randomize_doc_size=self.randomize_doc_size,
                                   randomize_value=self.randomize_value)
        gen_create2 = doc_generator("eviction2_",
                                    start=0,
                                    end=self.num_items,
                                    key_size=self.key_size,
                                    doc_size=self.doc_size,
                                    doc_type=self.doc_type,
                                    randomize_doc_size=self.randomize_doc_size,
                                    randomize_value=self.randomize_value)
        def_bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        remote = RemoteMachineShellConnection(self.cluster.master)
        cb_cli = CbCli(remote)
        for bucket in self.cluster.buckets:
            cb_cli.edit_bucket(bucket.name, compressionMode="off")
        remote.disconnect()
        self.sleep(10, "Wait for new compressionMode to reflect")

        # Load data and check stats to see compression
        # is not done for newly added data
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create2,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items*2)

    def MB36948(self):
        node_to_stop = self.servers[0]
        self.log.info("Adding index/query node")
        self.task.rebalance(self.cluster, [self.servers[2]], [],
                            services=["n1ql,index"])
        self.log.info("Creating SDK client connection")
        client = SDKClient(self.cluster, self.cluster.buckets[0],
                           compression_settings=self.sdk_compression)

        self.log.info("Stopping memcached on: %s" % node_to_stop)
        ssh_conn = RemoteMachineShellConnection(node_to_stop)
        err_sim = CouchbaseError(self.log,
                                 ssh_conn,
                                 node=node_to_stop)
        err_sim.create(CouchbaseError.STOP_MEMCACHED)

        result = client.crud(DocLoading.Bucket.DocOps.CREATE,
                             "abort1", "abort1_val")
        if not result["status"]:
            self.log_failure("Async SET failed")

        result = client.crud(DocLoading.Bucket.DocOps.UPDATE,
                             "abort1", "abort1_val",
                             durability=self.durability_level,
                             timeout=3, time_unit="seconds")
        if result["status"]:
            self.log_failure("Sync write succeeded")
        if SDKException.DurabilityAmbiguousException not in result["error"]:
            self.log_failure("Invalid exception for sync_write: %s" % result)

        self.log.info("Resuming memcached on: %s" % node_to_stop)
        err_sim.revert(CouchbaseError.STOP_MEMCACHED)

        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, 1)

        self.log.info("Closing ssh & SDK connections")
        ssh_conn.disconnect()
        client.close()

        self.validate_test_failure()

    def test_xattr_read_with_data_reader_permission(self):
        """
        Ref: MB-54776
        """
        user = "test_user"
        rbac_util = RbacUtil()
        testuser = [{'id': user, 'name': user, 'password': 'password'}]
        rolelist = [{'id': user, 'name': user, 'roles': 'data_reader[*]'}]
        try:
            rbac_util.remove_user_role([user],
                                       MembaseRestConnection(self.cluster.master))
        except Exception as e:
            if "User was not found." not in str(e):
                raise e

        self.log.info("Creating user '%s' with data_reader persmission" % user)
        rbac_util.create_user_source(testuser, 'builtin', self.cluster.master)
        status = rbac_util.add_user_role(
            rolelist, MembaseRestConnection(self.cluster.master), 'builtin')
        self.assertEqual(status[0]["id"], user, "User create failed")

        key = "test"
        bucket = self.cluster.buckets[0]
        client = SDKClient(self.cluster, bucket)
        insert_option = SDKOptions.get_insert_options()
        insert_option["transcoder"] = RawJSONTranscoder()

        client.collection.insert(key, "null", insert_option)
        client.crud(DocLoading.Bucket.SubDocOps.INSERT, key,
                    ["_xattr", "test_val"], xattr=True)
        client.close()

        client = SDKClient(self.cluster, bucket, username=user)
        success, fail = client.crud(DocLoading.Bucket.SubDocOps.LOOKUP, key,
                                    "$XTOC", xattr=True)
        client.close()
        self.assertIn(key, success, "$XTOC lookup failed: %s" % fail)
        # crud() returns the lookup result keyed by the requested path.
        # A data_reader must not see the system xattr, so $XTOC has to come
        # back as an empty list
        xtoc = success[key]['value']['$XTOC']
        self.assertEqual([], xtoc, "Value mismatch: %s" % xtoc)

    def test_defragmenter_sleep_time(self):
        """
        Ref: MB-55943
        With validate_frag_backpressure=True, also covers MB-55537
        (see _validate_frag_backpressure). It runs first on the empty
        bucket, so the slab layout it builds is not disturbed by the
        MB-55943 doc loads
        """
        result = True
        bucket = self.cluster.buckets[0]
        iterations = self.input.param("iterations", 2000)
        validate_frag_backpressure = self.input.param(
            "validate_frag_backpressure", False)
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)

        if validate_frag_backpressure:
            if len(kv_nodes) != 1:
                self.fail("Fragmentation back-pressure validation needs a "
                          f"single KV node, found {len(kv_nodes)}")
            self._validate_frag_backpressure(bucket, kv_nodes[0])

        doc_gen = doc_generator(self.key, 0, self.num_items,
                                key_size=self.key_size, doc_size=self.doc_size)
        self.log.info("Loading initial data load")
        load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.UPDATE,
            exp=self.maxttl, timeout_secs=60, durability=self.durability_level,
            process_concurrency=8, batch_size=500, print_ops_rate=False,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(load_task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        for node in kv_nodes:
            shell = RemoteMachineShellConnection(node)
            cb_stat = Cbstats(node)
            all_stats = cb_stat.all_stats(bucket.name)
            num_moved = int(all_stats["ep_defragmenter_num_moved"])
            num_visited = int(all_stats["ep_defragmenter_num_visited"])
            self.log.info("{0} - ep_defragmenter_num_moved={1}, "
                          "ep_defragmenter_num_visited={2}"
                          .format(node.ip, num_moved, num_visited))
            if num_visited == 0:
                result = False
                self.log.critical("{0} - ep_defragmenter_num_visited={1}"
                                  .format(node.ip, num_visited))
            cb_stat.disconnect()
            shell.disconnect()
        self.assertTrue(result, "Stat validation failed")

        self.log.info("Loading data for fragmentation")
        load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.UPDATE,
            exp=self.maxttl, timeout_secs=60, durability=self.durability_level,
            iterations=iterations, process_concurrency=8,
            batch_size=500, print_ops_rate=False,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(load_task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        for node in kv_nodes:
            shell = RemoteMachineShellConnection(node)
            cb_stat = Cbstats(node)
            all_stats = cb_stat.all_stats(bucket.name)
            num_moved = int(all_stats["ep_defragmenter_num_moved"])
            num_visited = int(all_stats["ep_defragmenter_num_visited"])
            self.log.info("{0} - ep_defragmenter_num_moved={1}, "
                          "ep_defragmenter_num_visited={2}"
                          .format(node.ip, num_moved, num_visited))
            if num_moved == 0:
                result = False
                self.log.critical("{0} - ep_defragmenter_num_moved={1}"
                                  .format(node.ip, num_moved))
            elif num_visited == 0:
                result = False
                self.log.critical("{0} - ep_defragmenter_num_visited={1}"
                                  .format(node.ip, num_visited))
            cb_stat.disconnect()
            shell.disconnect()
        self.assertTrue(result, "Stat validation failed")

    def test_couch_dbdump_json_with_non_utf8_keys(self):
        """
        Ref: MB-73006
        Data service does not need the doc keys to be valid UTF-8, but
        'couch_dbdump --json' aborted with nlohmann type_error.316 on the
        first such key. With the fix, the invalid bytes are emitted as
        U+FFFD and the record is flagged with "id_not_utf8": true.
        Non-JSON doc bodies get the same U+FFFD replacement.

        1. Write docs with valid / non-UTF-8 keys using memcached client
           (SDKs take str keys, which are always encoded as valid UTF-8)
        2. Wait for the docs to get persisted
        3. Run 'couch_dbdump --json' with / without --no-body on the
           vbucket files holding these docs
        4. Validate the dump completes, every line is a JSON record and
           only the non-UTF-8 keys are flagged with id_not_utf8
        """
        bucket = self.cluster.buckets[0]
        # Populates bucket.uuid, used as the bucket's data dir name
        self.bucket_util.get_all_buckets(self.cluster)
        if bucket.storageBackend != Bucket.StorageBackend.couchstore:
            self.fail("couch_dbdump needs a couchstore bucket, got "
                      f"{bucket.storageBackend}")
        # (name, key suffix, doc body, is the key valid UTF-8)
        doc_specs = [
            ("ascii", b"", b"body", True),
            ("multi_byte", b"\xc3\xa9", b"body", True),
            ("incomplete_seq", b"\xc9", b"body", False),
            ("invalid_bytes", b"\xff\xfe", b"body", False),
            ("lone_continuation", b"\x80_mid", b"body", False),
            ("overlong", b"\xc0\xaf", b"body", False),
            ("raw_body", b"", b"raw_\xc9_body", True)]

        vb_owner = {}
        for node in self.cluster_util.get_kv_nodes(self.cluster):
            for vb_num in Cbstats(node).vbucket_list(bucket.name, "active"):
                vb_owner[vb_num] = node
        num_vbuckets = len(vb_owner)

        docs_by_vb = {}
        for name, suffix, body, valid_key in doc_specs:
            # mc_bin_client hashes the key itself (fails for bytes keys)
            # when vbucket=0 is passed, so pick a key outside vbucket 0
            index = 0
            while True:
                stem = f"mb73006_{name}_{index}_"
                key = stem.encode() + suffix
                vb_num = ((zlib.crc32(key) >> 16) & 0x7fff) \
                    & (num_vbuckets - 1)
                if vb_num != 0:
                    break
                index += 1
            docs_by_vb.setdefault(vb_num, []).append(
                {"stem": stem, "key": key, "body": body,
                 "valid_key": valid_key})

        self.log.info("Writing docs with valid and non-UTF-8 keys")
        clients = {}
        try:
            for vb_num, docs in docs_by_vb.items():
                node = vb_owner[vb_num]
                if node not in clients:
                    clients[node] = MemcachedClientHelper.direct_client(
                        node, bucket)
                for doc in docs:
                    self.log.info(f"vb={vb_num}, key={doc['key']}")
                    clients[node].set(doc["key"], 0, 0, doc["body"],
                                      vbucket=vb_num)
        finally:
            for client in clients.values():
                client.close()
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])

        for vb_num, docs in docs_by_vb.items():
            node = vb_owner[vb_num]
            # Bucket data dir is named by the bucket's uuid (MB-66178)
            vb_dir = f"{MembaseRestConnection(node).get_data_path()}" \
                     f"/{bucket.uuid}"
            shell = RemoteMachineShellConnection(node)
            output, error = shell.execute_command(
                f"ls -1v {vb_dir}/{vb_num}.couch.* | tail -1")
            if not output:
                shell.disconnect()
                self.fail(f"{node.ip} - No couch file for vb {vb_num} "
                          f"in {vb_dir}: {error}")
            couch_file = output[0].strip()

            for dump_opts in ["--json --no-body", "--json"]:
                dump_cmd = f"{Linux.COUCHBASE_BIN_PATH}couch_dbdump " \
                           f"{dump_opts} {couch_file}"
                self.log.info(f"{node.ip} - Running '{dump_cmd}'")
                output, error = shell.execute_command(
                    f"{dump_cmd}; echo EXIT_CODE=$?")

                exit_code = None
                records = []
                for line in output:
                    line = line.strip()
                    if line.startswith("EXIT_CODE="):
                        exit_code = int(line.split("=")[1])
                    elif line:
                        try:
                            records.append(json.loads(line))
                        except ValueError:
                            self.log_failure(f"'{dump_cmd}' printed a "
                                             f"non-JSON line: {line}")
                if exit_code != 0:
                    self.log_failure(f"'{dump_cmd}' failed. "
                                     f"exit_code={exit_code}, "
                                     f"stderr={error}")

                for doc in docs:
                    matches = [record for record in records
                               if doc["stem"] in record.get("id", "")]
                    if len(matches) != 1:
                        self.log_failure(f"'{dump_cmd}' has {len(matches)} "
                                         f"records for key {doc['key']}")
                        continue
                    record = matches[0]
                    self.log.info(f"Key {doc['key']} -> {record}")
                    if doc["valid_key"]:
                        if not record["id"].endswith(doc["key"].decode()) \
                                or "id_not_utf8" in record:
                            self.log_failure(f"Valid UTF-8 key {doc['key']} "
                                             f"dumped as {record}")
                    elif "\ufffd" not in record["id"] \
                            or record.get("id_not_utf8") is not True:
                        self.log_failure(f"Non-UTF-8 key {doc['key']} not "
                                         f"replaced / flagged: {record}")
                    if "--no-body" not in dump_opts:
                        exp_body = doc["body"].decode("utf-8",
                                                      errors="replace")
                        if record.get("body") != exp_body:
                            self.log_failure(f"Key {doc['key']} body "
                                             f"mismatch. Expected "
                                             f"'{exp_body}': {record}")
            shell.disconnect()

    @staticmethod
    def _regions_per_slab(allocator_dump, min_size):
        """
        Regions-per-slab of the smallest jemalloc bin that fits 'min_size',
        read from the 'bins:' table of the server's jemalloc dump.
        Table columns are,
          size ind allocated nmalloc (#/sec) ndalloc (#/sec) nrequests
          (#/sec) nshards curregs curslabs nonfull_slabs regs pgs util ..
        so 'size' is column 0 and 'regs' is column 13.
        Returns 0 if the table could not be parsed
        """
        in_bins = False
        for line in allocator_dump.splitlines():
            if "bins:" in line:
                # Header line, data rows follow
                in_bins = True
                continue
            if not in_bins:
                continue
            cols = line.split()
            # Skip blank / '---' / section end / non-data rows
            if len(cols) < 14 or not cols[0].isdigit():
                continue
            if int(cols[0]) >= min_size:
                return int(cols[13])
        return 0

    def _validate_frag_backpressure(self, bucket, node):
        """
        Ref: MB-55537
        Port of kv_engine's testapp MemTrackingBucketTest.HighFragmentation

        Builds external jemalloc fragmentation till the bucket's resident
        memory (RSS) is over its quota while allocated stays under it,
        then validates,
        1. Gate: With fragmentation_backpressure_enabled, mutations are
           rejected with ETMPFAIL and each rejection is counted in both
           ep_fragmentation_tmp_ooms and ep_tmp_oom_errors
        2. Recovery: Defragmenter runs in aggressive mode (sleep drops to
           defragmenter_auto_min_sleep, age threshold 0), RSS returns under
           the quota and mutations are accepted again

        Fragmentation is built using two jemalloc size classes, so the
        holes in the first one are not refilled by the second:
         - Load class-A docs (1KB values -> 1280 bin) up to 0.7x quota
         - Delete all but one survivor per A slab (stride = regs of the
           bin), so each slab stays resident while its allocated collapses
         - Load class-B docs (4KB values -> 5120 bin) till RSS > 1.1x quota
         - Same survivor-per-slab delete on B
        Allocated is kept under 0.75x quota, so mem_used stays under the
        high_wat and the item pager never ejects the pinned values.

        Uses a single memcached connection with sequential mutations, so
        consecutive docs are allocated from the same slab.
        Expects: Single KV node (all vbuckets active locally), couchstore
        bucket with 100MB quota and compression_mode=off
        """
        chunk = 1000
        timeout = 60
        probe_key = "frag_backpressure_probe"
        cm_wake_key = "frag_checkpoint_recovery_wake"
        # Random letters keep the values incompressible, so each Blob
        # lands in the size class picked by its length
        value_a = "".join(choice(string.ascii_letters) for _ in range(1024))
        value_b = "".join(choice(string.ascii_letters) for _ in range(4096))

        client = MemcachedClientHelper.direct_client(node, bucket)
        stats = client.stats()
        if "ep_fragmentation_backpressure_enabled" not in stats:
            client.close()
            self.fail("ep_fragmentation_backpressure_enabled not found. "
                      "Server build does not have the MB-55537 changes")
        quota = int(stats["ep_max_size"])
        orig_flush_params = {
            "defragmenter_enabled": stats["ep_defragmenter_enabled"],
            "defragmenter_age_threshold":
                stats["ep_defragmenter_age_threshold"],
            "fragmentation_backpressure_enabled":
                stats["ep_fragmentation_backpressure_enabled"]}
        orig_checkpoint_mem_ratio = stats["ep_checkpoint_memory_ratio"]

        def wait_for(predicate, poll_interval=1.0):
            end_time = time.time() + timeout
            while time.time() < end_time:
                if predicate():
                    return True
                time.sleep(poll_interval)
            return False

        def set_flush_param(key, value):
            client.set_param(key, str(value), ENGINE_PARAM_FLUSH)

        def arena_memory():
            # (resident, allocated) from a single stat snapshot
            arena_stats = client.stats()
            return (int(arena_stats["ep_arena_memory_resident"]),
                    int(arena_stats["ep_arena_memory_allocated"]))

        def tmp_oom_counters():
            # (fragmentation, total) from a single stat snapshot
            oom_stats = client.stats()
            return (int(oom_stats["ep_fragmentation_tmp_ooms"]),
                    int(oom_stats["ep_tmp_oom_errors"]))

        def log_state(stage):
            mem_stats = client.stats()
            rss = int(mem_stats["ep_arena_memory_resident"])
            alloc = int(mem_stats["ep_arena_memory_allocated"])
            frag = 100.0 * (rss - alloc) / rss if rss > alloc else 0.0
            self.log.info(
                f"{stage} :: quota={quota}, resident={rss}, "
                f"allocated={alloc}, frag={frag:.2f}%, "
                f"items={mem_stats['curr_items']}, "
                f"mem_used={mem_stats['mem_used']}, "
                f"high_wat={mem_stats['ep_mem_high_wat']}, "
                f"value_ejects={mem_stats['ep_num_value_ejects']}, "
                f"tmp_ooms={mem_stats['ep_tmp_oom_errors']}, "
                f"frag_tmp_ooms={mem_stats['ep_fragmentation_tmp_ooms']}")

        def checkpoint_memory():
            cm_stats = client.stats()
            return (int(cm_stats["ep_checkpoint_memory"])
                    + int(cm_stats["ep_checkpoint_memory_pending_destruction"]))

        def recover_checkpoint_memory():
            # Lowering checkpoint_memory_ratio runs checkpoint memory
            # recovery to free the deleted Blobs still referenced from the
            # checkpoints. Setting the ratio does not wake the recovery task,
            # the next mutation does (and gets ETMPFAIL while checkpoint
            # memory is over the lowered quota), so wake it with a tiny doc
            # which stays out of the A/B size classes.
            # Ratio is restored once the checkpoint memory settles. Left at
            # 0.01, every later mutation gets the checkpoint-full ETMPFAIL
            # (kv_engine's testapp hides it with auto-retry on a single vb
            # bucket), which also cannot be told apart from the gate's
            # ETMPFAIL in the probes
            client.set_param("checkpoint_memory_ratio", "0.01",
                             ENGINE_PARAM_CHECKPOINT)
            try:
                try:
                    client.set(cm_wake_key, 0, 0, "x")
                except MemcachedError as e:
                    if e.status != ERR_ETMPFAIL:
                        raise
                last_seen = {"mem": None}

                def settled():
                    # First call only records, so each comparison is a
                    # poll_interval apart and not before the task has run
                    prev_mem = last_seen["mem"]
                    last_seen["mem"] = checkpoint_memory()
                    return prev_mem is not None \
                        and last_seen["mem"] >= prev_mem

                if not wait_for(settled):
                    self.log.warning("Checkpoint memory still dropping, "
                                     f"now at {last_seen['mem']}")
                self.log.info("Checkpoint memory after recovery: "
                              f"{last_seen['mem']}")
            finally:
                client.set_param("checkpoint_memory_ratio",
                                 orig_checkpoint_mem_ratio,
                                 ENGINE_PARAM_CHECKPOINT)

        def delete_all_but_one_per_slab(key_prefix, num_keys, value_len):
            # Stored Blob is value + ~9 bytes header. Both value lengths sit
            # on a size-class boundary, so the Blob spills into the next
            # class (1280 / 5120). +64 bumps the lookup into that class
            nregs = self._regions_per_slab(
                client.stats("allocator")["allocator"], value_len + 64)
            if nregs == 0:
                self.fail("Could not parse regions-per-slab for size "
                          f"{value_len + 64}")
            self.log.info(f"Deleting all but one '{key_prefix}' doc per "
                          f"slab, regions_per_slab={nregs}")
            for index in range(num_keys):
                if index % nregs != 0:
                    client.delete(f"{key_prefix}{index}")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         [bucket])
            recover_checkpoint_memory()

        def probe_rejected():
            try:
                client.set(probe_key, 0, 0, value_a)
            except MemcachedError as e:
                if e.status == ERR_ETMPFAIL:
                    return True
                raise
            return False

        try:
            # Defragmenter is disabled so it does not compact the
            # fragmentation being built and the recovery cannot pull RSS
            # under quota before the gate is observed.
            # Gate is disabled (default=true), else the class-B load gets
            # temp-OOMed once RSS crosses the quota, before reaching the
            # required fragmentation
            set_flush_param("defragmenter_enabled", "false")
            set_flush_param("fragmentation_backpressure_enabled", "false")
            frag_ooms_at_start, _ = tmp_oom_counters()
            log_state("Start")

            self.log.info("Loading class-A docs till allocated >= 0.7x quota")
            a_count = 0
            while arena_memory()[1] < quota * 7 // 10:
                for _ in range(chunk):
                    client.set(f"frag_a_{a_count}", 0, 0, value_a)
                    a_count += 1
            log_state(f"Loaded {a_count} class-A docs")
            delete_all_but_one_per_slab("frag_a_", a_count, len(value_a))
            log_state("After class-A deletes")

            self.log.info("Loading class-B docs till resident > 1.1x quota")
            b_count = 0
            rss, alloc = arena_memory()
            while rss <= quota * 11 // 10 and alloc < quota * 3 // 4:
                for _ in range(chunk):
                    client.set(f"frag_b_{b_count}", 0, 0, value_b)
                    b_count += 1
                rss, alloc = arena_memory()
            log_state(f"Loaded {b_count} class-B docs")
            delete_all_but_one_per_slab("frag_b_", b_count, len(value_b))
            log_state("After class-B deletes")

            rss, alloc = arena_memory()
            if rss <= quota or alloc >= quota:
                self.fail("Not in allocated < quota < resident state. "
                          f"quota={quota}, resident={rss}, allocated={alloc}")

            stats = client.stats()
            defrag_moved_before = int(stats["ep_defragmenter_num_moved"])
            min_sleep = float(stats["ep_defragmenter_auto_min_sleep"])
            if int(stats["ep_fragmentation_tmp_ooms"]) != frag_ooms_at_start:
                self.log_failure("ep_fragmentation_tmp_ooms moved while "
                                 "fragmentation_backpressure_enabled=false")

            # Gate: Defragmenter is still disabled, so RSS stays over quota.
            # MonitorTask publishes RSS/fragmentation every
            # monitor_task_interval, so poll till the probe is rejected
            self.log.info("Enabling fragmentation_backpressure_enabled")
            set_flush_param("fragmentation_backpressure_enabled", "true")
            if not wait_for(probe_rejected):
                self.fail("Mutation not rejected with ETMPFAIL though "
                          "resident > quota with high fragmentation")
            frag_ooms, total_ooms = tmp_oom_counters()
            if frag_ooms <= frag_ooms_at_start:
                self.log_failure("Mutation rejected but "
                                 "ep_fragmentation_tmp_ooms did not move")
            if not probe_rejected():
                self.log_failure("Latched gate accepted a mutation")
            new_frag_ooms, new_total_ooms = tmp_oom_counters()
            if new_frag_ooms != frag_ooms + 1:
                self.log_failure(f"ep_fragmentation_tmp_ooms {frag_ooms} -> "
                                 f"{new_frag_ooms} for one rejection")
            if new_total_ooms != total_ooms + 1:
                self.log_failure(f"ep_tmp_oom_errors {total_ooms} -> "
                                 f"{new_total_ooms} for one fragmentation "
                                 "rejection")
            log_state("Gate latched")

            # Recovery: With max age threshold (255) the normal mode cannot
            # move anything, so num_moved going up proves the aggressive
            # mode (age threshold 0) ran
            self.log.info("Enabling defragmenter to recover")
            set_flush_param("defragmenter_age_threshold", 255)
            set_flush_param("defragmenter_enabled", "true")
            last_seen = {}

            def in_aggressive_mode():
                defrag_stats = client.stats()
                last_seen["moved"] = \
                    int(defrag_stats["ep_defragmenter_num_moved"])
                last_seen["sleep"] = \
                    float(defrag_stats["ep_defragmenter_sleep_time"])
                return last_seen["moved"] > defrag_moved_before \
                    and last_seen["sleep"] <= min_sleep

            # Min sleep lasts only ~min_sleep secs, so poll at min_sleep/10
            if not wait_for(in_aggressive_mode, min_sleep / 10):
                self.fail("Defragmenter not seen in aggressive mode. "
                          f"num_moved={last_seen.get('moved')} "
                          f"(before={defrag_moved_before}), "
                          f"sleep_time={last_seen.get('sleep')} "
                          f"(min={min_sleep})")
            if not wait_for(lambda: arena_memory()[0] <= quota):
                self.fail("Resident did not return under quota. "
                          f"quota={quota}, resident={arena_memory()[0]}")
            if not wait_for(lambda: not probe_rejected()):
                self.fail("Gate did not lift after resident < quota")
            log_state("After recovery")

            frag_ooms, total_ooms = tmp_oom_counters()
            rejections = sum(1 for _ in range(10) if probe_rejected())
            new_frag_ooms, new_total_ooms = tmp_oom_counters()
            if rejections != 0:
                self.log_failure("Gate re-latched after recovery. "
                                 f"{rejections}/10 mutations rejected")
            if new_frag_ooms != frag_ooms or new_total_ooms != total_ooms:
                self.log_failure(
                    "Temp-OOM counters moved on accepted mutations. "
                    f"ep_fragmentation_tmp_ooms {frag_ooms} -> "
                    f"{new_frag_ooms}, ep_tmp_oom_errors {total_ooms} -> "
                    f"{new_total_ooms}")
        finally:
            try:
                for key, value in orig_flush_params.items():
                    set_flush_param(key, value)
                client.set_param("checkpoint_memory_ratio",
                                 orig_checkpoint_mem_ratio,
                                 ENGINE_PARAM_CHECKPOINT)
            except Exception as e:
                self.log_failure(f"Failed to restore bucket params: {e}")
            client.close()

    def test_compaction_on_expiry_load(self):
        """
        Ref: MB-53898
        """
        def bg_fetch_op(op_type, gen):
            client = self.cluster.sdk_client_pool.get_client_for_bucket(bucket)
            while compaction_running and gen.has_next():
                k, _ = gen.next()
                result = client.crud(op_type, k, {}, timeout=2)
                if result["status"] is False and \
                        SDKException.AmbiguousTimeoutException in result["error"] and \
                        SDKException.RetryReason.KV_TEMPORARY_FAILURE in result["error"]:
                    self.crud_failure = True
                    self.log.critical(result)
                    break
            self.cluster.sdk_client_pool.release_client(client)

        exp = 300
        self.crud_failure = False
        bucket = self.cluster.buckets[0]
        non_ttl_docs = int(self.num_items * 1.8)
        init_gen = doc_generator(self.key, 0, non_ttl_docs,
                                 doc_size=100, key_size=self.key_size)
        exp_gen = doc_generator("exp_docs", 0, self.num_items,
                                key_size=self.key_size)
        self.log.info("Loading non-ttl documents")
        init_load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, init_gen, DocLoading.Bucket.DocOps.CREATE,
            timeout_secs=300, durability=self.durability_level,
            process_concurrency=10, batch_size=2000, print_ops_rate=False,
            load_using=self.load_docs_using)
        self.log.info("Loading ttl documents")
        load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, exp_gen, DocLoading.Bucket.DocOps.CREATE,
            exp=exp, timeout_secs=300, durability=self.durability_level,
            process_concurrency=10, batch_size=2000, print_ops_rate=False,
            load_using=self.load_docs_using)
        self.log.info("Waiting for doc_loading to complete")
        self.task_manager.get_task_result(init_load_task)
        self.task_manager.get_task_result(load_task)

        self.sleep(exp, "Wait for docs to expire")
        compaction_running = True
        self.log.info("Starting create threads")

        gen_num = int(non_ttl_docs / 4)
        gen_1 = doc_generator(self.key, 0, gen_num, key_size=self.key_size,
                              doc_size=100)
        gen_2 = doc_generator(self.key, gen_num, gen_num * 2,
                              key_size=self.key_size, doc_size=100)
        gen_3 = doc_generator(self.key, gen_num * 2, gen_num * 3,
                              key_size=self.key_size, doc_size=100)
        gen_4 = doc_generator(self.key, gen_num * 3, gen_num * 4,
                              key_size=self.key_size, doc_size=100)
        op_threads = [
            Thread(target=bg_fetch_op,
                   args=(DocLoading.Bucket.DocOps.REPLACE, gen_1)),
            Thread(target=bg_fetch_op,
                   args=(DocLoading.Bucket.DocOps.READ, gen_2)),
            Thread(target=bg_fetch_op,
                   args=(DocLoading.Bucket.DocOps.REPLACE, gen_3)),
            Thread(target=bg_fetch_op,
                   args=(DocLoading.Bucket.DocOps.READ, gen_4))]

        for thread in op_threads:
            thread.start()

        self.sleep(5, "Wait for load threads to start")
        self.log.info("Running compaction on the bucket")
        c_task = self.task.async_compact_bucket(self.cluster.master, bucket)
        self.task_manager.get_task_result(c_task)
        compaction_running = False

        for thread in op_threads:
            thread.join(10)

        self.assertFalse(self.crud_failure, "Crud failure observed")

    def _compaction_stats(self, client):
        """
        MB-73521 compaction stats (+ vbucket counts) of the bucket selected
        on the memcached client, from one STAT call so that all of them
        describe the same instant
        """
        stat_keys = ("ep_compaction_succeeded", "ep_compaction_failed",
                     "ep_compaction_aborted", "ep_pending_compactions",
                     "ep_running_internal_compactions",
                     "ep_running_external_compactions",
                     "vb_active_num", "vb_replica_num")
        stats = client.stats()
        missing = [key for key in stat_keys if key not in stats]
        if missing:
            self.fail(f"Stats {missing} not found, needs a build with the "
                      f"MB-73521 fix")
        return {key: int(stats[key]) for key in stat_keys}

    def _sample_compaction_stats(self, clients, stop_sampling):
        """
        Polls _compaction_stats() of every KV node back to back, till
        stop_sampling is set. A running gauge is > 0 only while a vbucket
        is inside compactDB, so this polls over memcached and not cbstats
        (SSH + process start) to keep the gaps between samples short.
        Clients must not be used elsewhere till the sampler is joined.
        :param clients: {node_ip: memcached client, bucket selected}
        :return: (sampler thread, {node_ip: [stats, ...]}, errors)
        """
        samples = {ip: [] for ip in clients}
        errors = []

        def sample():
            try:
                while not stop_sampling.is_set():
                    for ip, client in clients.items():
                        samples[ip].append(self._compaction_stats(client))
            except Exception as e:
                errors.append(str(e))

        sampler = Thread(target=sample)
        sampler.start()
        return sampler, samples, errors

    def _wait_for_compactions_to_drain(self, samples, errors, timeout=300):
        """
        Waits till every node's latest sample has no compaction scheduled
        or running, after having seen one scheduled
        """
        def drained(node_samples):
            last = node_samples[-1]
            return any(sample["ep_pending_compactions"]
                       for sample in node_samples) \
                and last["ep_pending_compactions"] == 0 \
                and last["ep_running_internal_compactions"] == 0 \
                and last["ep_running_external_compactions"] == 0

        end_time = time.time() + timeout
        while not errors and time.time() < end_time:
            if all(node_samples and drained(node_samples)
                   for node_samples in samples.values()):
                return
            time.sleep(1)
        self.fail(f"Compactions not drained in {timeout}s, sampler errors: "
                  f"{errors}")

    def _disable_auto_compaction(self, bucket):
        """
        Fragmentation thresholds at 100%, so ns_server never compacts the
        bucket by itself and adds to the counted compactions.
        bucket_util.disable_compaction() passes None for them, which leaves
        ns_server's 30% default in place
        """
        self.bucket_util.modify_fragmentation_config(
            self.cluster, {"dbFragmentThresholdPercentage": 100,
                           "viewFragmentThresholdPercentage": 100},
            bucket.name)

    def _set_compaction_concurrency_ratio(self, bucket, kv_nodes, ratio):
        """
        compaction_max_concurrent_ratio of the bucket on every KV node.
        0 caps it to the minimum of one compaction at a time
        """
        for node in kv_nodes:
            shell = RemoteMachineShellConnection(node)
            Cbepctl(shell).set(bucket.name, "flush_param",
                               "compaction_max_concurrent_ratio", ratio)
            shell.disconnect()

    def _load_and_update_docs(self, bucket, scope=CbServer.default_scope,
                              collection=CbServer.default_collection):
        """
        Creates num_items docs and updates them once, so a compaction has
        stale versions to drop, then waits for persistence
        """
        for op_type in [DocLoading.Bucket.DocOps.CREATE,
                        DocLoading.Bucket.DocOps.UPDATE]:
            load_gen = doc_generator(self.key, 0, self.num_items,
                                     doc_size=self.doc_size)
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, load_gen, op_type,
                batch_size=500, process_concurrency=8,
                scope=scope, collection=collection, print_ops_rate=False,
                load_using=self.load_docs_using)
            self.task_manager.get_task_result(task)
            if task.fail:
                self.fail(f"Doc {op_type} failed for {len(task.fail)} keys")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

    def _validate_running_compactions(self, ip, node_samples, trigger):
        """
        With the concurrency capped to 1, the 'trigger' running gauge must
        have been seen at 1, never above it, never above the pending count
        it is a subset of, while the gauge of the other trigger stays 0
        """
        running_stat = f"ep_running_{trigger}_compactions"
        other_stat = "ep_running_%s_compactions" \
                     % ("internal" if trigger == "external" else "external")
        running = [sample[running_stat] for sample in node_samples]
        self.log.info(f"{ip} :: {len(node_samples)} samples, "
                      f"{running_stat} seen at 1 in "
                      f"{running.count(1)} samples")
        if max(running) != 1:
            self.log_failure(f"{ip}: {running_stat} peaked at "
                             f"{max(running)}, expected 1 as compaction "
                             f"concurrency is capped to 1")
        over_pending = [sample for sample in node_samples
                        if sample[running_stat]
                        > sample["ep_pending_compactions"]]
        if over_pending:
            self.log_failure(f"{ip}: {running_stat} above "
                             f"ep_pending_compactions: {over_pending[:3]}")
        if any(sample[other_stat] for sample in node_samples):
            self.log_failure(f"{ip}: {other_stat} went above 0 though only "
                             f"{trigger} compactions were requested")

    def _validate_compaction_outcomes(self, ip, start, end,
                                      expected_succeeded):
        """
        ep_compaction_succeeded went up by expected_succeeded, one per
        vbucket compaction, and nothing failed or aborted
        """
        succeeded = end["ep_compaction_succeeded"] \
            - start["ep_compaction_succeeded"]
        self.log.info(f"{ip} :: ep_compaction_succeeded +{succeeded}")
        if succeeded != expected_succeeded:
            self.log_failure(f"{ip}: ep_compaction_succeeded went up by "
                             f"{succeeded}, expected {expected_succeeded}")
        for stat in ["ep_compaction_failed", "ep_compaction_aborted"]:
            if end[stat] != start[stat]:
                self.log_failure(f"{ip}: {stat} {start[stat]} -> "
                                 f"{end[stat]}")

    def _validate_compaction_prometheus(self, node, bucket, succeeded):
        """
        KV Prometheus exports ep_compaction_succeeded as it is, and both
        running gauges as the family ep_running_compactions{trigger}, which
        must be 0 with no compaction left
        """
        pattern = re.compile(r"(kv_ep_compaction_succeeded"
                             r"|kv_ep_running_compactions)\{([^}]*)\} (\S+)")
        exported = {}
        for line in StatsHelper(node).get_prometheus_metrics(
                component="kv"):
            match = pattern.match(line.strip())
            if match and f'bucket="{bucket.name}"' in match.group(2):
                trigger = re.search(r'trigger="(\w+)"', match.group(2))
                metric = match.group(1)
                if trigger:
                    metric += f"/{trigger.group(1)}"
                exported[metric] = float(match.group(3))
        expected = {"kv_ep_compaction_succeeded": succeeded,
                    "kv_ep_running_compactions/internal": 0,
                    "kv_ep_running_compactions/external": 0}
        self.log.info(f"{node.ip} :: Prometheus {exported}")
        if exported != expected:
            self.log_failure(f"{node.ip}: Prometheus exported {exported}, "
                             f"expected {expected}")

    def test_compaction_outcome_and_running_stats(self):
        """
        Ref: MB-73521
        ep_running_external_compactions counts the externally requested
        (ns_server / CompactDB) vbucket compactions only while inside
        compactDB, unlike ep_pending_compactions which also counts the ones
        queued behind the concurrency limit. ep_compaction_succeeded counts
        each vbucket compaction run to completion, next to
        ep_compaction_failed / ep_compaction_aborted.

        1. Disable auto-compaction, load docs and update them once
        2. Cap the bucket's compaction concurrency to 1 on every KV node,
           so the CompactDB requests from ns_server queue up
        3. Compact the bucket while sampling the stats over memcached
        4. Validate per KV node,
           - running_external seen at 1, never above 1 or pending,
             running_internal stays 0
           - pending seen above running (the queued compactions)
           - succeeded went up by the node's vbuckets, nothing failed or
             aborted
           - Prometheus kv_ep_compaction_succeeded matches cbstats and
             kv_ep_running_compactions is 0 for both triggers
        """
        bucket = self.cluster.buckets[0]
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        self._disable_auto_compaction(bucket)
        self._load_and_update_docs(bucket)
        self._set_compaction_concurrency_ratio(bucket, kv_nodes, 0)

        clients = {node.ip: MemcachedClientHelper.direct_client(node, bucket)
                   for node in kv_nodes}
        try:
            start = {ip: self._compaction_stats(client)
                     for ip, client in clients.items()}
            stop_sampling = Event()
            sampler, samples, errors = self._sample_compaction_stats(
                clients, stop_sampling)
            try:
                self.log.info("Compacting the bucket")
                compaction_task = self.task.async_compact_bucket(
                    self.cluster.master, bucket)
                self.task_manager.get_task_result(compaction_task)
                self._wait_for_compactions_to_drain(samples, errors)
            finally:
                stop_sampling.set()
                sampler.join()
            end = {ip: self._compaction_stats(client)
                   for ip, client in clients.items()}
        finally:
            for client in clients.values():
                client.close()
        self.assertTrue(compaction_task.result, "Bucket compaction failed")

        for node in kv_nodes:
            node_samples = samples[node.ip]
            self._validate_running_compactions(node.ip, node_samples,
                                               "external")
            if not any(sample["ep_pending_compactions"]
                       > sample["ep_running_external_compactions"] > 0
                       for sample in node_samples):
                self.log_failure(f"{node.ip}: ep_pending_compactions never "
                                 f"seen above the running ones, queued "
                                 f"compactions not told apart")
            self._validate_compaction_outcomes(
                node.ip, start[node.ip], end[node.ip],
                expected_succeeded=start[node.ip]["vb_active_num"]
                + start[node.ip]["vb_replica_num"])
            self._validate_compaction_prometheus(
                node, bucket, end[node.ip]["ep_compaction_succeeded"])
        self.validate_test_failure()

    def test_running_internal_compactions_on_collection_drop(self):
        """
        Ref: MB-73521
        KV schedules compactions by itself to purge a dropped collection
        (collections_drop_compaction_delay later), which must be counted
        by ep_running_internal_compactions, never by the external one.
        Till the delay elapses the snoozed compactions are seen only in
        ep_pending_compactions.

        1. Disable auto-compaction, load docs into a new collection and
           update them once
        2. Cap the bucket's compaction concurrency to 1 on every KV node
        3. Drop the collection while sampling the stats over memcached
        4. Once KV is done, validate per KV node,
           - pending seen > 0 with nothing running (snoozed compactions)
           - running_internal seen at 1, never above 1 or pending,
             running_external stays 0
           - succeeded went up by the node's vbuckets, nothing failed or
             aborted
           - Prometheus kv_ep_running_compactions is 0 for both triggers
        """
        bucket = self.cluster.buckets[0]
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        scope = CbServer.default_scope
        collection = "dropped_collection"
        self._disable_auto_compaction(bucket)
        self.bucket_util.create_collection(self.cluster.master, bucket, scope,
                                           {"name": collection})
        self._load_and_update_docs(bucket, scope, collection)
        self._set_compaction_concurrency_ratio(bucket, kv_nodes, 0)

        clients = {node.ip: MemcachedClientHelper.direct_client(node, bucket)
                   for node in kv_nodes}
        try:
            start = {ip: self._compaction_stats(client)
                     for ip, client in clients.items()}
            stop_sampling = Event()
            sampler, samples, errors = self._sample_compaction_stats(
                clients, stop_sampling)
            try:
                self.log.info(f"Dropping collection {scope}.{collection}")
                self.bucket_util.drop_collection(self.cluster.master, bucket,
                                                 scope, collection)
                self._wait_for_compactions_to_drain(samples, errors)
            finally:
                stop_sampling.set()
                sampler.join()
            end = {ip: self._compaction_stats(client)
                   for ip, client in clients.items()}
        finally:
            for client in clients.values():
                client.close()

        for node in kv_nodes:
            node_samples = samples[node.ip]
            if not any(sample["ep_pending_compactions"] > 0
                       and sample["ep_running_internal_compactions"] == 0
                       for sample in node_samples):
                self.log_failure(f"{node.ip}: Snoozed compactions not seen "
                                 f"as pending with none running")
            self._validate_running_compactions(node.ip, node_samples,
                                               "internal")
            # num_items puts docs of the collection in every vbucket, so
            # every vbucket gets a purge compaction
            self._validate_compaction_outcomes(
                node.ip, start[node.ip], end[node.ip],
                expected_succeeded=start[node.ip]["vb_active_num"]
                + start[node.ip]["vb_replica_num"])
            self._validate_compaction_prometheus(
                node, bucket, end[node.ip]["ep_compaction_succeeded"])
        self.validate_test_failure()

    def test_hash_table_during_compaction_expiry(self):
        """
        - Load TTL / non-TTL docs in parallel and wait for docs to persist
        - Read the TTL docs to make sure they are marked as expired
        - Do this in a loop for few times
        - Trigger compaction and validate the temp_hash_items to zero
        Ref: MB-61250
        """
        doc_gen = doc_generator(self.key, 0, self.num_items)
        bucket = self.cluster.buckets[0]
        shell = RemoteMachineShellConnection(self.cluster.master)
        cb_stats = Cbstats(self.cluster.master)

        non_ttl_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen,
            DocLoading.Bucket.DocOps.CREATE, batch_size=1000,
            process_concurrency=2, print_ops_rate=False,
            skip_read_on_error=True, suppress_error_table=True,
            iterations=-1,
            load_using=self.load_docs_using)

        self.log.info("Loading ttl docs for 50 iterations")
        for _ in range(50):
            for i in range(5):
                ttl_doc_gen = doc_generator("exp_docs_%s" % i, 0, 10000,
                                            key_size=self.key_size,
                                            doc_size=self.doc_size)
                l_task = self.task.async_load_gen_docs(
                    self.cluster, bucket, ttl_doc_gen,
                    DocLoading.Bucket.DocOps.UPDATE, exp=10, batch_size=2000,
                    process_concurrency=3, print_ops_rate=False,
                    skip_read_on_error=True, suppress_error_table=True,
                    iterations=3,
                    load_using=self.load_docs_using)
                self.task_manager.get_task_result(l_task)

        # Wait for load to complete
        non_ttl_task.end_task()
        self.task_manager.get_task_result(non_ttl_task)

        # Wait for items to get persisted
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)

        self.log.info("Running manual compaction for the bucket")
        self.bucket_util._run_compaction(self.cluster)

        self.log.info("Validating stats after compaction")
        all_stats = cb_stats.all_stats(bucket.name)
        vb_stats = cb_stats.vbucket_details(bucket.name)
        cb_stats.disconnect()
        shell.disconnect()

        self.assertTrue(int(all_stats["ep_bg_fetched_compaction"]) > 1000,
                        "ep_bg_fetched_compaction %s < 1000"
                        % all_stats["ep_bg_fetched_compaction"])
        total_docs = 0
        for vb_num, stats in vb_stats.items():
            total_docs += int(stats["ht_num_items"])
            self.assertEqual(int(stats["ht_num_temp_items"]), 0,
                             "vb-%s :: ht_num_temp_items %s != 0"
                             % (vb_num, stats["ht_num_temp_items"]))

    def test_oso_backfill_not_sending_duplicate_items(self):
        """
        1. Create few collections and load initial data
        2. Load further data and create indexing on one particular collection
        3. Make sure the mutations are received only once by index-dcp

        Ref: MB-57106
        """

        def set_and_validate_dcp_oso_backfill(t_node, backfill_val):
            shell = RemoteMachineShellConnection(t_node)
            cbstats = Cbstats(t_node)
            if backfill_val in ["enabled", "disabled"]:
                cbepctl = Cbepctl(shell)
                cbepctl.set(bucket.name, "dcp_param", "dcp_oso_backfill",
                            backfill_val)
            val = cbstats.all_stats(bucket.name)["ep_dcp_oso_backfill"]
            self.log.info("Validate dcp_oso_backfill={}".format(backfill_val))
            self.assertEqual(
                val, backfill_val,
                "ep_dcp_oso_backfill {} != {}".format(val, backfill_val))
            cbstats.disconnect()
            shell.disconnect()

        oso_backfill_enabled = self.input.param("oso_backfill_enabled", None)
        bucket = self.cluster.buckets[0]
        rest = MembaseRestConnection(self.cluster.master)
        self.log.info("Setting index mem. quota=256M")
        rest.set_service_mem_quota({CbServer.Settings.INDEX_MEM_QUOTA: 256})
        self.log.info("Setting indexerThreads=1 for creating DCP pause/resume")
        rest.set_indexer_params(indexerThreads=1)
        c_index = 1
        num_items = self.num_items
        c_dict = {CbServer.default_collection: self.num_items * 2}
        while num_items != 0:
            c_name = "c{}".format(c_index)
            self.log.info("Creating collections %s" % c_name)
            c_dict[c_name] = num_items
            num_items = int(num_items / 2)
            self.bucket_util.create_collection(self.cluster.master, bucket,
                                               collection_spec={"name": c_name})
            if num_items == c_dict[c_name]:
                break
            c_index += 1

        tasks = list()
        for c_name, num_items in c_dict.items():
            self.log.info("Loading {} items into {} collection"
                          .format(num_items, c_name))
            doc_gen = doc_generator("random_keys", 0, num_items,
                                    key_size=15, doc_size=1024,
                                    randomize_doc_size=True,
                                    randomize_value=True, deep_copy=True)
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.CREATE,
                collection=c_name, batch_size=1000, process_concurrency=1,
                print_ops_rate=False, load_using=self.load_docs_using))
        for task in tasks:
            self.task_manager.get_task_result(task)

        # Wait for docs to persisted
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)

        for kv_node in self.cluster_util.get_kv_nodes(self.cluster):
            self.log.info("Enabling dcp::oso_backfill on {}"
                          .format(kv_node.ip))
            if oso_backfill_enabled is True:
                set_and_validate_dcp_oso_backfill(kv_node, "enabled")
            elif oso_backfill_enabled is False:
                set_and_validate_dcp_oso_backfill(kv_node, "disabled")
            else:
                set_and_validate_dcp_oso_backfill(kv_node, "auto")

        # Index build is deliberately slowed down (indexerThreads=1 +
        # 256M index quota) to create the DCP pause/resume this test needs,
        # so allow it a few minutes to come online
        index_timeout = self.input.param("index_timeout", 600)

        self.log.info("Creating GSI index on collection 'c1'")
        client = SDKClient(self.cluster, bucket)
        # QueryResult is lazy - the statement is only sent to the server once
        # the rows are consumed. Without the list() the DDL is never executed
        # and the index simply never appears
        list(client.run_query(
            "CREATE INDEX `c1` ON `{}`.`_default`.`c1`(body) USING GSI"
            .format(bucket.name), timeout=index_timeout))
        query = "SELECT state FROM system:indexes WHERE name='c1'"
        state = None
        stop_time = time.time() + index_timeout
        while time.time() < stop_time:
            rows = list(client.cluster.query(query))
            if rows:
                state = rows[0].get("state")
                if state == "online":
                    break
            self.sleep(2, "Waiting for index 'c1' to come online")
        client.close()
        if state != "online":
            self.fail("Index 'c1' not online within %ss (state=%s)"
                      % (index_timeout, state))
        self.log.info("Index created")
        self.sleep((c_dict["c1"] * 2) / 10000,
                   "Wait before fetching index stats")
        gsi_rest = GsiHelper(self.cluster.servers[2], self.log)
        stats = gsi_rest.get_index_stats()
        dict_key = "{}:{}:c1:c1".format(bucket.name, CbServer.default_scope)
        for field in ["num_docs_indexed", "items_count", "num_items_flushed",
                      "num_flush_queued"]:
            t_val = stats["{}:{}".format(dict_key, field)]
            self.assertEqual(t_val, c_dict["c1"],
                             "Mismatch in index stat {} :: {} != {}"
                             .format(field, t_val, self.num_items))

    def test_expel_non_meta_items_from_checkpoint(self):
        """
        Ref: MB-39344
        """
        cp_mem_ratio = self.input.param("checkpoint_mem_ratio", "0.1")
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbepctl = Cbepctl(shell)

        self.log.info("Loading data into all vbuckets")
        shell.execute_command(
            "/opt/couchbase/bin/cbc-pillowfight -u Administrator -P password "
            "-U couchbase://localhost/%s -I 10 -m 20000000 -M 20000000 -c 2"
            % self.cluster.buckets[0])
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        self.log.info("Setting checkpoint_memory_ratio=%s" % cp_mem_ratio)
        cbepctl.set(self.cluster.buckets[0].name, "checkpoint_param",
                    "checkpoint_memory_ratio", str(cp_mem_ratio))
        shell.disconnect()

        self.sleep(10, "WAIT")
        self.log.info("Rebalance-in %s nodes" % self.num_replicas)
        result = self.task.rebalance(
            self.cluster, self.cluster.servers[1:1+self.num_replicas], [])
        self.assertTrue(result, "Rebalance failed")

    def test_unlock_key(self):
        """
        Ref: MB-58088 / MB-59060 / MB-59746 / MB-64973
        """
        def validate_unlock_exception(t_key, d_cas, expected_errors):
            try:
                client.collection.unlock(t_key, d_cas)
            except AmbiguousTimeoutException as e:
                if "key_value_temporary_failure" in str(e):
                    if return_tmpfail_val == "false":
                        self.fail(f"Key '{t_key}' - KV_TEMPORARY_FAILURE")
                    else:
                        # Expected since not_locked_returns_tmpfail is enabled
                        pass
            except CouchbaseException as e:
                err_found = False
                for exp_err in expected_errors:
                    if exp_err in str(e):
                        err_found = True
                        break
                self.assertTrue(err_found,
                                f"Key '{t_key}' - Unexpected error: {e}")

        if self.bucket_type == Bucket.Type.EPHEMERAL:
            self.log.critical("Test not applicable for Ephemeral buckets")
            return

        key_1 = "test_doc_1"
        key_2 = "test_doc_2"
        key_3 = "test_doc_3"
        bucket = self.cluster.buckets[0]
        if self.cluster.sdk_client_pool:
            client = self.cluster.sdk_client_pool.get_client_for_bucket(bucket)
        else:
            client = SDKClient(self.cluster, bucket)

        nodes = {"shell": dict(),
                 "cbstats": dict()}

        rest = ClusterRestAPI(self.cluster.master)
        # Create required shell and cbstats connections
        for kv_node in self.cluster.kv_nodes:
            nodes["shell"][kv_node] = RemoteMachineShellConnection(kv_node)
            nodes["shell"][kv_node].enable_diag_eval_on_non_local_hosts()
            nodes["cbstats"][kv_node] = Cbstats(kv_node)

        for index, return_tmpfail_val in enumerate(["false", "true", "false"]):
            self.log.info(f"Flusing {bucket.name} to test"
                          f" return_tmpfail_val={return_tmpfail_val}")
            self.bucket_util.flush_bucket(self.cluster, bucket)
            self.sleep(5, "Wait for flush to complete")

            # MB-64973: index==0 is default value in server side
            if index != 0:
                self.log.info("Setting not_locked_returns_tmpfail="
                              f" {return_tmpfail_val}")
                rest.ns_bucket_update_bucket_props(
                    bucket.name,
                    f"not_locked_returns_tmpfail={return_tmpfail_val}")
                for kv_node in self.cluster.kv_nodes:
                    nodes["shell"][kv_node].restart_couchbase()

                self.sleep(15, "Wait for servers to restart")
                okay = True
                for kv_node in self.cluster.kv_nodes:
                    okay = self.cluster_util.wait_for_ns_servers(
                        kv_node, wait_time=60) and okay
                self.assertTrue(okay, "Servers not fully up")

            if return_tmpfail_val == "true":
                not_locked_msgs = ["key_value_temporary_failure"]
            else:
                not_locked_msgs = ["Requested resource is not locked",
                                   "NOT_LOCKED"]

            for kv_node in self.cluster.kv_nodes:
                all_stats = nodes["cbstats"][kv_node].all_stats(bucket.name)
                if all_stats["ep_not_locked_returns_tmpfail"] \
                        != return_tmpfail_val:
                    self.fail(f"{kv_node.ip} - "
                              f"Unexpected val {return_tmpfail_val}")

            self.log.info("Test for multiple doc-unlock")
            result = client.crud(DocLoading.Bucket.DocOps.UPDATE, key_1, {})
            original_cas = result["cas"]
            result = client.collection.get_and_lock(
                key_1, SDKOptions.get_duration(15, "seconds"))
            locked_cas = result.cas
            self.assertNotEqual(original_cas, locked_cas, "CAS not updated")
            client.collection.unlock(key_1, locked_cas)
            validate_unlock_exception(key_1, locked_cas, not_locked_msgs)
            result = client.crud(DocLoading.Bucket.DocOps.READ, key_1)
            cas_after_unlock = result["cas"]
            self.assertEqual(original_cas, cas_after_unlock, "CAS updated")

            self.log.info("Testing unlock without lock")
            cas = client.crud(DocLoading.Bucket.DocOps.UPDATE, key_2,
                              {})["cas"]
            validate_unlock_exception(key_2, cas, not_locked_msgs)

            self.log.info("Testing with expired key")
            cas = client.crud(DocLoading.Bucket.DocOps.UPDATE,
                              key_3, {}, exp=2)["cas"]
            self.sleep(3, "Wait for doc_to_expire")
            validate_unlock_exception(key_3, cas,
                                      SDKException.DocumentNotFoundException)

            # Test with evicted docs
            self.log.info("Testing evicted keys with lock expired")
            client.crud(DocLoading.Bucket.DocOps.UPDATE, key_1, {})
            client.crud(DocLoading.Bucket.DocOps.UPDATE, key_2, {})
            doc_1_cas = client.collection.get_and_lock(
                key_1, SDKOptions.get_duration(15, "seconds")).cas
            doc_2_cas = client.collection.get_and_lock(
                key_2, SDKOptions.get_duration(15, "seconds")).cas
            doc_3_cas = client.crud(DocLoading.Bucket.DocOps.UPDATE,
                                    key_3, {}, exp=10)["cas"]
            self.log.info("Loading more docs for eviction to get trigger")
            doc_gen = doc_generator("non_ttl_keys", 0, 1000000,
                                    key_size=20, doc_size=1024)
            load_task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.UPDATE,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=100, process_concurrency=4, print_ops_rate=False,
                load_using=self.load_docs_using)
            self.task_manager.get_task_result(load_task)
            self.log.info("Validating unlock outcome with eviction")
            validate_unlock_exception(key_1, doc_1_cas, not_locked_msgs)
            validate_unlock_exception(key_2, doc_2_cas, not_locked_msgs)
            validate_unlock_exception(key_3, doc_3_cas,
                                      SDKException.DocumentNotFoundException)
            # Invalid CAS test
            validate_unlock_exception(key_1, doc_1_cas+1, not_locked_msgs)
            validate_unlock_exception(key_2, doc_2_cas+1, not_locked_msgs)
            validate_unlock_exception(key_3, doc_3_cas+1,
                                      SDKException.DocumentNotFoundException)

        self.log.info("Closing client connections")
        for kv_node in self.cluster.kv_nodes:
            nodes["shell"][kv_node].disconnect()
            nodes["cbstats"][kv_node].disconnect()

    def test_mutate_prepare_evict(self):
        """
        Ref: MB-60046
        """
        def perform_sync_write(sdk_client, doc_key):
            self.log.info("Creating prepare document")
            self.mutation_result = sdk_client.crud(
                DocLoading.Bucket.DocOps.UPDATE, doc_key, {},
                durability=SDKConstants.DurabilityLevel.MAJORITY,
                timeout=70)

        def load_docs(num_items):
            gen = doc_generator("test_docs", 0, num_items, key_size=100,
                                doc_size=1024)

            l_task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen, DocLoading.Bucket.DocOps.UPDATE,
                batch_size=20, process_concurrency=3, print_ops_rate=False,
                skip_read_on_error=True, suppress_error_table=True,
                load_using=self.load_docs_using)
            self.task_manager.get_task_result(l_task)

        cbstat = cb_err = None
        active_vbs = None
        key = "test_key"
        bucket = self.cluster.buckets[0]
        if self.cluster.sdk_client_pool:
            client = self.cluster.sdk_client_pool.get_client_for_bucket(bucket)
        else:
            client = SDKClient(self.cluster, bucket)
        vb_for_key = self.bucket_util.get_vbucket_num_for_key(
            key, bucket.numVBuckets)

        self.log.info("Disabling auto-failover settings")
        ClusterRestAPI(self.cluster.master)\
            .update_auto_failover_settings("false", 60)

        load_docs(10000)
        perform_sync_write(client, key)
        load_docs(1000)
        for node in self.cluster.nodes_in_cluster:
            cbstat = Cbstats(node)
            active_vbs = cbstat.vbucket_list(bucket.name)
            replica_vbs = cbstat.vbucket_list(bucket.name,
                                              Bucket.vBucket.REPLICA)
            if vb_for_key in replica_vbs:
                self.log.critical("Stopping memcached on %s" % node.ip)
                cb_err = CouchbaseError(self.log, cbstat.shellConn)
                cb_err.create(CouchbaseError.STOP_MEMCACHED)
                break
            cbstat.disconnect()

        target_vbs = list(set(range(0, 1024)) - set(active_vbs))
        doc_gen = doc_generator("test_docs", 0, 100000, key_size=220,
                                doc_size=1024, target_vbucket=target_vbs,
                                vbuckets=bucket.numVBuckets)

        load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.UPDATE,
            batch_size=20, process_concurrency=3, print_ops_rate=False,
            skip_read_on_error=True, suppress_error_table=True,
            start_task=False, load_using=self.load_docs_using)

        prepare_mutation_thread = Thread(target=perform_sync_write,
                                         args=[client, key])
        prepare_mutation_thread.start()

        self.sleep(1, "Wait for prepare mutation to initiate")
        self.log.info("Starting data load to tigger eviction")
        self.task_manager.add_new_task(load_task)
        self.task_manager.get_task_result(load_task)
        if self.cluster.sdk_client_pool:
            self.cluster.sdk_client_pool.release_client(client)
        else:
            client.close()
        self.log.info("Reverting error condition")
        cb_err.revert(CouchbaseError.STOP_MEMCACHED)
        cbstat.shellConn.disconnect()

        self.sleep(5, "Wait before validating hash_table")
        hash_dump_cmd = \
            "%s -u %s -p %s localhost:%d raw \"_hash-dump %d\" | grep %s" \
            % (Linux.COUCHBASE_BIN_PATH + "cbstats",
               self.cluster.master.rest_username,
               self.cluster.master.rest_password,
               self.cluster.master.memcached_port, vb_for_key, key)

        for node in self.cluster.nodes_in_cluster:
            if node.ip != cbstat.shellConn.ip:
                t_shell = RemoteMachineShellConnection(node)
                output = t_shell.execute_command(hash_dump_cmd)[0][0]
                t_shell.disconnect()
                self.assertTrue(output.find("..J W.R.Cp. temp:") > 0,
                                "Unexpected hash_table output: %s" % output)
                self.assertTrue(output.find(" del_time:") == -1,
                                "Unexpected hash_table output: %s" % output)

    def test_backfill_during_warmup_to_load_active_vbs(self):
        """
        - Create a cluster with two or more nodes.
        - Load data greater than memory.
        - Restart the nodes.
        - After warmup, all nodes should have high active RR
          (consistent with data volume) and low replica RR.

        Ref: MB-59817
        """
        node_info = dict()
        bucket = self.cluster.buckets[0]
        load_gen = doc_generator(self.key, 0, self.num_items*10,
                                 key_size=100, doc_size=2048)
        self.log.info("Loading documents into the bucket")
        load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, load_gen, DocLoading.Bucket.DocOps.UPDATE,
            durability=self.durability_level, batch_size=200, iterations=5,
            process_concurrency=8, print_ops_rate=False,
            skip_read_on_error=True, suppress_error_table=True,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(load_task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])

        self.log.info("Creating shell connections")
        for node in self.cluster.nodes_in_cluster:
            node_info[node.ip] = dict()
            node_info[node.ip]["shell"] = RemoteMachineShellConnection(node)
            node_info[node.ip]["cbstat"] = Cbstats(node)
            stats = node_info[node.ip]["cbstat"].all_stats(bucket.name)
            active_rr_perc = float(stats["vb_active_perc_mem_resident"])
            replica_rr_perc = float(stats["vb_replica_perc_mem_resident"])
            self.log.info("{} - vb_active_perc_mem_resident: {}, "
                          "vb_replica_perc_mem_resident: {}"
                          .format(node.ip, active_rr_perc, replica_rr_perc))

        self.log.info("Killing memecached on nodes")
        for _, n_info in node_info.items():
            cb_err = CouchbaseError(self.log, n_info["shell"])
            cb_err.create(CouchbaseError.KILL_MEMCACHED)

        self.bucket_util._wait_warmup_completed(bucket,
                                                self.cluster.nodes_in_cluster)
        self.sleep(5, "Wait for memcached to complete warmup")
        for ip, n_info in node_info.items():
            stats = n_info["cbstat"].all_stats(bucket.name)
            active_rr_perc = float(stats["vb_active_perc_mem_resident"])
            replica_rr_perc = float(stats["vb_replica_perc_mem_resident"])
            self.log.info("{} - vb_active_perc_mem_resident: {}, "
                          "vb_replica_perc_mem_resident: {}"
                          .format(ip, active_rr_perc, replica_rr_perc))
            if active_rr_perc < replica_rr_perc:
                self.fail("{} - Replica_RR :: {} < {} :: Active_RR"
                          .format(ip, replica_rr_perc, active_rr_perc))

        self.log.info("Closing connections")
        for _, n_info in node_info.items():
            n_info["cbstat"].disconnect()
            n_info["shell"].disconnect()

    def test_data_eviction_under_low_water_mark(self):
        """
        Ref: MB-64008
        1. Load data just below low watermark on Ephemeral bucket
        2. Perform graceful failover
        3. Make sure the num_items remain the same
        """
        def validate_item_count():
            item_count = None
            retry = 10
            while retry > 0:
                _, b_info = b_rest.get_bucket_info(bucket.name)
                item_count = int(b_info["basicStats"]["itemCount"])
                if self.num_items == item_count:
                    break
                retry -= 1
                self.sleep(2, "Wait before next check")
            else:
                self.fail("Bucket item_count mismatch: expected %s, got %s"
                          % (self.num_items, item_count))

        def max_mem_pct_of_low_wm():
            """Highest mem_used, as a % of low_wm, across the KV nodes"""
            worst = 0
            for t_node in self.cluster.nodes_in_cluster:
                t_stats = node_info[t_node.ip]["cbstat"].all_stats(bucket.name)
                pct = int(int(t_stats["mem_used"]) * 100
                          / int(t_stats["ep_mem_low_wat"]))
                self.log.info("%s - Low wm: %s, Mem used: %s (%s%%)"
                              % (t_node.ip, t_stats["ep_mem_low_wat"],
                                 t_stats["mem_used"], pct))
                worst = max(worst, pct)
            return worst

        node_info = dict()
        bucket = self.cluster.buckets[0]
        doc_size = self.input.param("doc_size", 2048)
        # The premise is 'data sits just below the low watermark'. Per-item
        # memory cost depends on bucket type, compression and quota, so a
        # hard-coded (bucket_size, num_items) pair silently tips over the
        # watermark whenever either changes - and on an nruEviction bucket
        # crossing it auto-deletes items, which shows up as a doc_count
        # mismatch rather than as 'too much data'. Load in batches and stop
        # on the measured watermark instead, treating num_items as a cap
        load_batch = self.input.param("load_batch", 50000)
        low_wm_pct = self.input.param("low_wm_pct", 85)

        for node in self.cluster.nodes_in_cluster:
            node_info[node.ip] = {"cbstat": Cbstats(node)}

        self.log.info("Loading documents into the bucket")
        loaded = 0
        while loaded < self.num_items:
            batch = min(load_batch, self.num_items - loaded)
            load_gen = doc_generator("docs", loaded, loaded + batch,
                                     key_size=20, doc_size=doc_size)
            load_task = self.task.async_load_gen_docs(
                self.cluster, bucket, load_gen,
                DocLoading.Bucket.DocOps.CREATE,
                batch_size=200, process_concurrency=4, print_ops_rate=False,
                skip_read_on_error=True)
            self.task_manager.get_task_result(load_task)
            if load_task.fail:
                self.fail("Load failed for %s of %s docs. Sample: %s"
                          % (len(load_task.fail), batch,
                             list(load_task.fail.items())[:3]))
            loaded += batch
            mem_pct = max_mem_pct_of_low_wm()
            if mem_pct >= low_wm_pct:
                self.log.info("Stopping load at %s docs - mem_used is at "
                              "%s%% of low_wm (cap %s%%)"
                              % (loaded, mem_pct, low_wm_pct))
                break
        # Everything downstream compares against what actually got loaded
        self.num_items = loaded
        self.log.info("Loaded %s docs" % self.num_items)

        self.log.info("Validation doc_count")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])
        rest = ClusterRestAPI(self.cluster.master)
        b_rest = BucketRestApi(self.cluster.master)
        validate_item_count()

        self.log.info("Fetching cb_stats")
        max_mem_pct_of_low_wm()

        fo_node_ip = self.cluster.servers[1].ip
        otp_node = "ns_1@%s" % fo_node_ip
        self.log.info("Performing graceful failover of %s" % otp_node)
        rest.perform_graceful_failover(otp_node)
        self.assertTrue(RebalanceUtil(self.cluster).monitor_rebalance(),
                        "FO rebalance failed")
        validate_item_count()

        # nodes_in_cluster[0] can be the node that was just failed over, and
        # memcached there no longer serves the bucket - 'cbstats all' then
        # comes back with the bucket-less stat set (no ep_* keys) instead of
        # raising, which surfaces as a KeyError on ep_mem_low_wat. Read the
        # watermarks off a node that is still serving the bucket
        node = [t_node for t_node in self.cluster.nodes_in_cluster
                if t_node.ip != fo_node_ip][0]
        cbstat = node_info[node.ip]["cbstat"].all_stats(bucket.name)
        self.log.info("%s - Low wm: %s, Mem used: %s"
                      % (node.ip, cbstat["ep_mem_low_wat"],
                         cbstat["mem_used"]))
        for node in self.cluster.nodes_in_cluster:
            node_info[node.ip]["cbstat"].disconnect()

        rest.set_recovery_type(
            otp_node, recovery_type=CbServer.Failover.RecoveryType.DELTA)
        result = self.task.rebalance(self.cluster, [], [])
        self.assertTrue(result, "Delta recovery rebalance failed")
        validate_item_count()

    def test_ephemeral_auto_delete_pager_throttle(self):
        """
        Ref: MB-63560 — 2-node cluster scenario using a frozen replica as
        the lagging DCP cursor.

        Setup:
          - 2-node cluster (nodes_init >= 2), ephemeral NRU bucket with replicas=1.
          - node1 stays running; node2 memcached is SIGSTOP'd.

        Mechanism:
          When node2 is frozen, the DCP replica stream from node1 to node2 stalls.
          node1's checkpoint manager cannot free entries for VBuckets whose
          replica cursor is stuck. The item pager on node1:
            Run 1  — allowed (bytesPagerDeleted starts at 0 < pager_limit)
            Run 2+ — throttled (bytesPagerDeleted >= limit, cursor hasn't
                     advanced past pagedSeqno → pager_throttled increments)
          Memory stays above high_wm despite repeated pager invocations.

        Steps:
          1. Fail fast if nodes_init < 2.
          2. Read default watermarks from cbstats; query active VBuckets from
             both nodes.
          3. Pre-load N items uniformly across all VBuckets (both nodes up)
             to bring mem_used close to low_wm, reducing warm-up time.
          4. SIGSTOP node2 memcached.
          5. Incrementally load batches targeting only node1's active VBs
             (target_vbucket=) until pager_throttled has advanced across
             `throttle_confirmations` (default 3) separate batches, then stop
             loading and record the item count at that point. If KV rejects
             >= `max_reject_pct` (default 50) % of a batch while mem_used is
             still under low_wm, fail right away: the pager has not started,
             so the precondition cannot be reached. Also fail right away if
             the live item count collapses below `pager_retain_pct` % of its
             peak during the load (over-deletion as it happens), or if KV
             keeps rejecting that share of a batch above low_wm.
          6. Sleep 60 s with pager stuck.
          7. SIGCONT node2 — replica DCP drains, pager resumes.
          8. Wait up to 120 s for mem_used to fall back under low_wm.
          9. Assert the item count on node1 is still at least
             `pager_retain_pct` (default 50) % of the count recorded in step 5
             - i.e. the pager did not over-delete.

        Note on step 5: once an nruEviction ephemeral bucket sits at its
        quota, each further create auto-deletes older items to make room, so
        the live item count should level off near capacity. It should not
        collapse: a count far below its peak while mem_used stays high means
        the pager's deletes are not releasing memory - the over-deletion
        this test exists to catch.
        """
        if self.nodes_init < 2:
            self.fail("test_ephemeral_auto_delete_pager_throttle requires "
                      "nodes_init >= 2")

        bucket = self.cluster.buckets[0]
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        node1 = kv_nodes[0]   # stays running — target for all loads
        node2 = kv_nodes[1]   # memcached will be SIGSTOP'd

        shell2 = RemoteMachineShellConnection(node2)
        cbstat1 = Cbstats(node1)
        cbstat2 = Cbstats(node2)
        err_sim = CouchbaseError(self.log, shell2, node=node2)

        low_wm_hit = False
        peak_active_items = 0
        doc_size = 1024
        load_batch = 50000       # docs per incremental round (node1 VBs only)
        max_load_batches = 100   # safety cap to avoid infinite loop
        # Wall-clock cap on the incremental-load loop. The batch cap alone is
        # not a bound: when the loads themselves fail (every op timing out at
        # timeout_secs=3 x batch_size=100) a single batch costs ~25min, so
        # 100 batches is ~42h - enough to blow past the Jenkins job timeout
        # and starve every test queued behind this one.
        load_timeout = self.input.param("pager_load_timeout", 2700)
        # Below low_wm the pager is not involved at all, so KV rejecting this
        # share of a batch there means memory pressure cannot be built and
        # there is no pager behaviour left to observe
        max_reject_pct = self.input.param("max_reject_pct", 50)
        retain_pct = self.input.param("pager_retain_pct", 50)

        def summarize_errors(failed_docs):
            """'<error type> x<count> (e.g. <first message>)' per type"""
            counts = {}
            samples = {}
            for val in failed_docs.values():
                err = val.get("error") if isinstance(val, dict) else val
                err_type = type(err).__name__ if isinstance(err, Exception) \
                    else str(err).split(":")[0][:80]
                counts[err_type] = counts.get(err_type, 0) + 1
                samples.setdefault(err_type, str(err)[:300])
            return "; ".join("%s x%d (e.g. %s)" % (e_type, cnt, samples[e_type])
                             for e_type, cnt in counts.items())

        def pressure_stats(t_stats):
            """Stats that explain why KV rejects writes before the pager"""
            return ", ".join(
                "%s=%s" % (stat, t_stats.get(stat, "n/a"))
                for stat in ["ep_checkpoint_memory",
                             "ep_checkpoint_memory_quota",
                             "ep_cursors_dropped", "ep_tmp_oom_errors",
                             "ep_oom_errors"])

        def auto_delete_stats(t_stats):
            """Ephemeral auto-delete / seqlist (tombstone) stats, matched by
            name so this keeps working if the exact stat set differs across
            builds - these show where mem_used sits once items are gone"""
            return ", ".join(
                "%s=%s" % (stat, t_stats[stat]) for stat in sorted(t_stats)
                if stat.startswith(("vb_active_", "ep_"))
                and ("auto_delete" in stat or "seqlist" in stat)) or "n/a"

        try:
            # ---- Read default watermarks from cbstats (no forced overrides) ----
            a_stats = cbstat1.all_stats(bucket.name)
            high_wm = int(a_stats["ep_mem_high_wat"])
            low_wm = int(a_stats["ep_mem_low_wat"])
            self.log.info("Watermarks from cbstats: "
                          "high_wm=%dMB low_wm=%dMB"
                          % (high_wm >> 20, low_wm >> 20))

            # ---- VBucket distribution (both nodes) ----
            node1_active_vbs = cbstat1.vbucket_list(bucket.name, "active")
            node2_active_vbs = cbstat2.vbucket_list(bucket.name, "active")
            self.log.debug("node1(%s): %d active VBs  node2(%s): %d active VBs"
                           % (node1.ip, len(node1_active_vbs),
                              node2.ip, len(node2_active_vbs)))

            # ---- SIGSTOP node2 memcached ----
            err_sim.create(CouchbaseError.STOP_MEMCACHED)

            # ---- Incremental load — node1 VBs only ----
            # target_vbucket filters doc_generator to emit only keys that hash
            # to node1's active VBs, so every write goes to the running node.
            # A range of 2×load_batch covers ~load_batch effective docs at the
            # ~50 % VBucket split.
            # Loading stops as soon as pager_throttled has advanced over
            # `throttle_confirmations` separate batches: that is the signal
            # the test is after, and every batch written past it only
            # auto-deletes older items (nruEviction on a bucket already at
            # quota), which has nothing to do with the throttle.
            a_stats = cbstat1.all_stats(bucket.name)
            mem_used = int(a_stats["mem_used"])
            throttle_confirmations = self.input.param(
                "throttle_confirmations", 3)
            prev_throttled = int(a_stats.get("pager_throttled", 0))
            confirmations = 0
            throttle_confirmed = False
            items_at_throttle = 0
            peak_mem_used = mem_used
            pressure_blocked = False
            over_deleted = False
            load_deadline = time.time() + load_timeout
            for batch_num in range(1, max_load_batches + 1):
                if time.time() > load_deadline:
                    # Reported by the throttle_confirmed check below
                    self.log.info(
                        "Stopping load: %ss budget exhausted after %d "
                        "batches - mem_used=%dMB low_wm=%dMB"
                        % (load_timeout, batch_num - 1,
                           mem_used >> 20, low_wm >> 20))
                    break
                self.key = uuid.uuid4().hex[:6]
                this_batch = load_batch
                doc_gen = doc_generator(
                    self.key, 0, this_batch,
                    key_size=self.key_size, doc_size=doc_size,
                    target_vbucket=node1_active_vbs)
                load_task = self.task.async_load_gen_docs(
                    self.cluster, bucket, doc_gen,
                    DocLoading.Bucket.DocOps.CREATE,
                    timeout_secs=3,
                    print_ops_rate=False, skip_read_on_error=True,
                    suppress_error_table=True,
                    batch_size=100, process_concurrency=1)
                self.task_manager.get_task_result(load_task)

                a_stats = cbstat1.all_stats(bucket.name)
                mem_used = int(a_stats["mem_used"])
                peak_mem_used = max(peak_mem_used, mem_used)
                if not low_wm_hit and mem_used > low_wm:
                    low_wm_hit = True
                    self.log.info("Low watermark level hit")
                    load_batch = 20000
                active_items = int(a_stats.get("vb_active_curr_items", 0))
                peak_active_items = max(peak_active_items, active_items)
                throttled = int(a_stats.get("pager_throttled", 0))
                rejected = len(load_task.fail)
                self.log.info(
                    "Batch %d: mem_used=%dMB high_wm=%dMB low_wm=%dMB "
                    "vb_active_curr_items=%d pager_throttled=%s "
                    "rejected=%d/%d (%s)"
                    % (batch_num, mem_used >> 20, high_wm >> 20,
                       low_wm >> 20, active_items, throttled,
                       rejected, this_batch, pressure_stats(a_stats)))
                self.log.info("Batch %d auto-delete stats: %s"
                              % (batch_num, auto_delete_stats(a_stats)))

                # Over-deletion as it happens: live items far below their
                # peak with the loader still writing means the pager is
                # deleting without freeing memory. Nothing later in the test
                # can recover from that, so report it here with the numbers
                # that show it instead of loading on until the time budget
                if peak_active_items \
                        and active_items * 100 < peak_active_items * retain_pct:
                    over_deleted = True
                    self.log_failure(
                        "Pager over-deleted during load on %s: "
                        "vb_active_curr_items fell to %d in batch %d from a "
                        "peak of %d (< %s%%) while mem_used=%dMB "
                        "(low_wm=%dMB high_wm=%dMB) pager_throttled=%d. "
                        "%s. Auto-delete stats: %s"
                        % (node1.ip, active_items, batch_num,
                           peak_active_items, retain_pct, mem_used >> 20,
                           low_wm >> 20, high_wm >> 20, throttled,
                           pressure_stats(a_stats),
                           auto_delete_stats(a_stats)))
                    break

                # Writes refused while still under low_wm are not the
                # pager's doing (it has not started yet). Every further
                # batch then only times out op by op (~25min per batch at
                # timeout_secs=3) without adding memory, so stop here and
                # report why the precondition could not be reached
                if not low_wm_hit \
                        and rejected * 100 >= this_batch * max_reject_pct:
                    pressure_blocked = True
                    self.log_failure(
                        "Could not build memory pressure on %s: KV rejected "
                        "%d of %d creates in batch %d at mem_used=%dMB, "
                        "below low_wm=%dMB, so the pager never ran and "
                        "there is no throttling to validate. %s. "
                        "Errors: %s"
                        % (node1.ip, rejected, this_batch, batch_num,
                           mem_used >> 20, low_wm >> 20,
                           pressure_stats(a_stats),
                           summarize_errors(load_task.fail)))
                    break
                if low_wm_hit \
                        and rejected * 100 >= this_batch * max_reject_pct:
                    pressure_blocked = True
                    self.log_failure(
                        "Bucket stopped accepting writes on %s: KV rejected "
                        "%d of %d creates in batch %d at mem_used=%dMB "
                        "(high_wm=%dMB) with vb_active_curr_items=%d. %s. "
                        "Errors: %s"
                        % (node1.ip, rejected, this_batch, batch_num,
                           mem_used >> 20, high_wm >> 20, active_items,
                           pressure_stats(a_stats),
                           summarize_errors(load_task.fail)))
                    break

                if throttled > prev_throttled:
                    confirmations += 1
                prev_throttled = throttled
                if confirmations >= throttle_confirmations:
                    throttle_confirmed = True
                    items_at_throttle = int(a_stats.get("curr_items", 0))
                    self.log.info(
                        "Pager throttling confirmed over %d batches "
                        "(pager_throttled=%d) - stopping load with "
                        "curr_items=%d"
                        % (confirmations, throttled, items_at_throttle))
                    break
            else:
                # Reported by the throttle_confirmed check below
                self.log.info(
                    "Stopping load: batch cap (%d) reached - "
                    "mem_used=%dMB low_wm=%dMB" % (max_load_batches,
                                                   mem_used >> 20,
                                                   low_wm >> 20))

            # ---- SIGCONT node2 — release DCP cursor ----
            err_sim.revert(CouchbaseError.STOP_MEMCACHED)

            # ---- Wait with pager stuck ----
            self.sleep(60, "Wait for eviction to complete")

            # ---- Wait for memory to come back under low_wm ----
            # Only an upper bound is asserted. Once the replica DCP cursor
            # is released the pager frees whatever it had already queued, so
            # mem_used settling *below* low_wm is the expected outcome - the
            # earlier '+/-20% of low_wm' window failed every healthy run that
            # drained past that band
            normalize_timeout = self.input.param("normalize_timeout", 120)
            deadline = time.time() + normalize_timeout
            normalized = False
            while time.time() < deadline:
                a_stats = cbstat1.all_stats(bucket.name)
                mem_used = int(a_stats["mem_used"])
                mem_pct = int(mem_used * 100 / low_wm)
                self.log.info(
                    "Waiting for normalization: mem_used=%dMB "
                    "(%d%% of low_wm=%dMB)"
                    % (mem_used >> 20, mem_pct, low_wm >> 20))
                if mem_used <= low_wm:
                    self.log.info(
                        "Memory normalized: mem_used=%dMB (%d%% of low_wm=%dMB)"
                        % (mem_used >> 20, mem_pct, low_wm >> 20))
                    normalized = True
                    break
                self.sleep(5)
            if not normalized:
                a_stats = cbstat1.all_stats(bucket.name)
                final_mem = int(a_stats["mem_used"])
                final_pct = int(final_mem * 100 / low_wm)
                self.log_failure(
                    "mem_used did not fall back under low_wm within %ss "
                    "after SIGCONT: mem_used=%dMB (%d%% of low_wm=%dMB)"
                    % (normalize_timeout, final_mem >> 20, final_pct,
                       low_wm >> 20))

            # ---- Validate item count ----
            # The floor is derived from what the node actually held when the
            # throttle kicked in, not from a fixed item count: how many items
            # fit under the watermarks depends on the bucket quota, key/doc
            # size and compression, so a hard-coded number only ever measures
            # the bucket config. `min_curr_items` remains available to pin an
            # absolute floor from the conf file
            min_curr_items = self.input.param(
                "min_curr_items", int(items_at_throttle * retain_pct / 100))
            final_stats = cbstat1.all_stats(bucket.name)
            curr_items = int(final_stats.get("curr_items", 0))
            self.log.info(
                "Final curr_items on node1: %d (was %d when the pager "
                "throttle was confirmed, floor is %d)"
                % (curr_items, items_at_throttle, min_curr_items))
            if not throttle_confirmed:
                # Both have already been reported from the load loop
                if not (pressure_blocked or over_deleted):
                    self.log_failure(
                        "pager_throttled did not advance over %d batches "
                        "while node2 memcached was frozen - peak "
                        "mem_used=%dMB low_wm=%dMB high_wm=%dMB "
                        "pager_throttled=%s"
                        % (throttle_confirmations, peak_mem_used >> 20,
                           low_wm >> 20, high_wm >> 20,
                           final_stats.get("pager_throttled", "n/a")))
            elif curr_items < min_curr_items:
                self.log_failure(
                    "Pager over-deleted: curr_items=%s after test (expected "
                    ">= %s, %s%% of the %s items held when throttling was "
                    "confirmed). Peak vb_active_curr_items during load was "
                    "%s, mem_used=%dMB low_wm=%dMB high_wm=%dMB "
                    "pager_throttled=%s"
                    % (curr_items, min_curr_items, retain_pct,
                       items_at_throttle, peak_active_items,
                       int(final_stats["mem_used"]) >> 20, low_wm >> 20,
                       high_wm >> 20,
                       final_stats.get("pager_throttled", "n/a")))
        finally:
            err_sim.revert(CouchbaseError.STOP_MEMCACHED)
            cbstat1.disconnect()
            cbstat2.disconnect()
            shell2.disconnect()

        self.validate_test_failure()

    def do_get_random_key(self):
        # MB-31548, get_Random key gets hung sometimes.
        mc = MemcachedClient(self.cluster.master.ip,
                             constants.memcached_port)
        mc.sasl_auth_plain(self.cluster.master.rest_username,
                           self.cluster.master.rest_password)
        mc.bucket_select('default')

        count = 0
        while count < 1000000:
            count += 1
            try:
                mc.get_random_key()
            except MemcachedError as error:
                self.fail(f"<MemcachedError #{error.status} `{error.msg}`>")
            if count % 1000 == 0:
                self.log.info('The number of iteration is {}'.format(count))

    def test_kv_curr_connections_bucket_metrics(self):
        """
        Validate bucket-level Prometheus metrics kv_curr_bucket_connections
        and kv_curr_bucket_connections_closing using 2 queries per node:
          - Both metrics must be present for every cluster bucket
          - kv_curr_bucket_connections must be non-negative
          - kv_curr_bucket_connections_closing must be 0 or 1 during
            stable test execution (no active teardowns expected)
        [P1] Bucket-level connection metric presence and sanity.
        """
        buckets = [b.name for b in self.cluster.buckets]

        # Open SDK connections to each bucket so the per-bucket connection
        # gauge is emitted (KV skips zero-connection buckets in the metric).
        clients = []
        try:
            for bucket in self.cluster.buckets:
                clients.append(SDKClient(self.cluster, bucket))

            # Wait for Prometheus to scrape the KV endpoint with the active
            # connections now open (scrape interval is ~10s).
            self.sleep(15, "waiting for Prometheus to scrape active connections")

            for server in self.cluster.kv_nodes:
                # --- Query 1: kv_curr_bucket_connections ---
                try:
                    _, res_conn = MembaseRestConnection(server).query_prometheus(
                        "kv_curr_bucket_connections")
                except Exception as e:
                    self.log.warning(
                        f"query_prometheus(kv_curr_bucket_connections) failed on "
                        f"{server.ip}: {e}")
                    res_conn = {}

                conn_results = res_conn.get("data", {}).get("result", [])
                conn_bucket_found = {b: False for b in buckets}

                for item in conn_results:
                    b = item["metric"].get("bucket")
                    if b not in conn_bucket_found:
                        continue
                    value = float(item["value"][1])
                    conn_bucket_found[b] = True
                    self.assertGreaterEqual(
                        value, 0,
                        f"kv_curr_bucket_connections[{b}] on {server.ip} "
                        f"is negative: {value}")
                    self.log.info(
                        f"{server.ip}: kv_curr_bucket_connections[{b}]={value}")

                missing = [b for b, found in conn_bucket_found.items()
                           if not found]
                self.assertFalse(
                    missing,
                    f"kv_curr_bucket_connections missing for buckets on "
                    f"{server.ip}: {missing}")

                # --- Query 2: kv_curr_bucket_connections_closing ---
                try:
                    _, res_closing = MembaseRestConnection(server).query_prometheus(
                        "kv_curr_bucket_connections_closing")
                except Exception as e:
                    self.log.warning(
                        f"query_prometheus(kv_curr_bucket_connections_closing) "
                        f"failed on {server.ip}: {e}")
                    res_closing = {}

                closing_results = res_closing.get("data", {}).get("result", [])
                closing_bucket_found = {b: False for b in buckets}

                for item in closing_results:
                    b = item["metric"].get("bucket")
                    if b not in closing_bucket_found:
                        continue
                    value = float(item["value"][1])
                    closing_bucket_found[b] = True
                    self.assertLessEqual(
                        value, 1,
                        f"kv_curr_bucket_connections_closing[{b}] on "
                        f"{server.ip} = {value} "
                        f"(expected 0 or 1 during stable test execution)")
                    self.log.info(
                        f"{server.ip}: kv_curr_bucket_connections_closing"
                        f"[{b}]={value}")

                missing_closing = [
                    b for b, found in closing_bucket_found.items() if not found]
                self.assertFalse(
                    missing_closing,
                    f"kv_curr_bucket_connections_closing missing for buckets "
                    f"on {server.ip}: {missing_closing}")

        finally:
            for c in clients:
                try:
                    c.close()
                except Exception:
                    pass
