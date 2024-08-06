import json
import urllib
from random import choice, randint
from threading import Thread

from couchbase.exceptions import CouchbaseException, AmbiguousTimeoutException

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket

from SecurityLib.rbac import RbacUtil
from basetestcase import ClusterSetup
from cb_constants import constants, CbServer, DocLoading
from cb_server_rest_util.rest_client import RestConnection
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
from platform_constants.os_constants import Linux
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

        # Set index storage to avoid failures during index creation
        rest = RestConnection(self.cluster.master)
        rest.activate_service_api(CbServer.Services.INDEX)
        rest.index.set_gsi_settings({"storageMode": "plasma"})

        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("==========Finished Basic_ops base setup========")

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def do_basic_ops(self):
        KEY_NAME = 'key1'
        KEY_NAME2 = 'key2'
        self.log.info('Starting basic ops')

        default_bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        sdk_client = SDKClient(self.cluster, default_bucket,
                               compression_settings=self.sdk_compression)
        # mcd = client.memcached(KEY_NAME)

        # MB-17231 - incr with full eviction
        rc = sdk_client.incr(KEY_NAME, delta=1)
        self.log.info('rc for incr: {0}'.format(rc))

        # MB-17289 del with meta
        rc = sdk_client.set(KEY_NAME, 0, 0,
                            json.dumps({'value': 'value2'}))
        self.log.info('set is: {0}'.format(rc))
        # cas = rc[1]

        # wait for it to persist
        persisted = 0
        while persisted == 0:
            opaque, rep_time, persist_time, persisted, cas = \
                sdk_client.observe(KEY_NAME)

        try:
            rc = sdk_client.evict_key(KEY_NAME)
        except MemcachedError as exp:
            self.fail("Exception with evict meta - {0}".format(exp))

        CAS = 0xabcd
        try:
            # key, exp, flags, seqno, cas
            rc = mcd.del_with_meta(KEY_NAME2, 0, 0, 2, CAS)
        except MemcachedError as exp:
            self.fail("Exception with del_with meta - {0}".format(exp))

    # Reproduce test case for MB-28078
    def do_setWithMeta_twice(self):
        mc = MemcachedClient(self.cluster.master.ip,
                             constants.memcached_port)
        mc.sasl_auth_plain(self.cluster.master.rest_username,
                           self.cluster.master.rest_password)
        mc.bucket_select('default')

        try:
            mc.setWithMeta('1', '{"Hello":"World"}', 3600, 0, 1,
                           0x1512a3186faa0000)
        except MemcachedError as error:
            self.log.info("<MemcachedError #%d ``%s''>"
                          % (error.status, error.message))
            self.fail("Error on First setWithMeta()")

        stats = mc.stats()
        self.log.info('curr_items: {0} and curr_temp_items:{1}'
                      .format(stats['curr_items'], stats['curr_temp_items']))
        self.sleep(5, "Wait before checking the stats")
        stats = mc.stats()
        self.log.info('curr_items: {0} and curr_temp_items:{1}'
                      .format(stats['curr_items'], stats['curr_temp_items']))

        try:
            mc.setWithMeta('1', '{"Hello":"World"}', 3600, 0, 1,
                           0x1512a3186faa0000)
        except MemcachedError as error:
            stats = mc.stats()
            self.log.info('After 2nd setWithMeta(), curr_items: {} '
                          'and curr_temp_items: {}'
                          .format(stats['curr_items'],
                                  stats['curr_temp_items']))
            if int(stats['curr_temp_items']) == 1:
                self.fail("Error on second setWithMeta(), "
                          "expected curr_temp_items to be 0")
            else:
                self.log.info("<MemcachedError #%d ``%s''>"
                              % (error.status, error.message))

    def generate_docs_bigdata(self, docs_per_day, start=0,
                              document_size=1024000):
        return doc_generator(self.key, start, docs_per_day,
                             key_size=self.key_size,
                             doc_size=document_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=self.cluster.vbuckets,
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
        ignore_exceptions = list()
        retry_exceptions = list()
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
            vbuckets=self.cluster.vbuckets,
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
            vbuckets=self.cluster.vbuckets,
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
            vbuckets=self.cluster.vbuckets,
            mutate=1,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value)

        if self.target_vbucket:
            mutation_doc_count = len(doc_update.doc_keys)
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
            total_updates = task.get_total_doc_ops()
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
            self.task.jython_task_manager.get_task_result(task)

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
            vbuckets=self.cluster.vbuckets,
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
        gens_load = self.generate_docs_bigdata(
            docs_per_day=self.num_items, document_size=self.doc_size)
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gens_load,
                DocLoading.Bucket.DocOps.CREATE, 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)

        # check if all the documents(250) are loaded with default timeout
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

    def test_large_doc_20MB(self):
        # test reproducer for MB-29258,
        # Load a doc which is greater than 20MB
        # with compression enabled and check if it fails
        # check with compression_mode as active, passive and off

        gens_load = self.generate_docs_bigdata(
            docs_per_day=1, document_size=(self.doc_size * 1024000))
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gens_load,
                DocLoading.Bucket.DocOps.CREATE, 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
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
                    docs_per_day=1, document_size=(21 * 1024000))
                task = self.task.async_load_gen_docs(
                    self.cluster, bucket, gens_update,
                    DocLoading.Bucket.DocOps.UPDATE, 0,
                    batch_size=10,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    durability=self.durability_level,
                    compression=self.sdk_compression,
                    timeout_secs=self.sdk_timeout,
                    load_using=self.load_docs_using)
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
        cmd = []
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
        bucket = self.cluster.buckets[0]
        doc_gen = doc_generator(self.key, 0, self.num_items,
                                doc_size=1)
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
            timeout_secs=self.sdk_timeout)
        self.sleep(60, "Wait for read task to start")
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
            self.sleep(120, "Total_gets: %s, itr: %s" % (total_gets,
                                                         read_task.itr_count))

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
        bucket = self.cluster.buckets[0]
        cb_stat_obj = dict()
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        for node in kv_nodes:
            cb_stat_obj[node] = Cbstats(node)

        doc_gen = doc_generator(self.key, 0, self.num_items, doc_size=1)
        create_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=500, process_concurrency=self.process_concurrency,
            timeout_secs=self.sdk_timeout)
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
            timeout_secs=self.sdk_timeout)

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
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(key)
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
        target_vb = choice(range(self.cluster.vbuckets))
        vb_str = str(target_vb)
        doc_gen = doc_generator(self.key, 0, self.num_items,
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
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(key)

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
            self.task_manager.get_task_result(doc_op_task)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            if op_type == DocLoading.Bucket.DocOps.CREATE:
                self.num_items += load_batch
            elif op_type == DocLoading.Bucket.DocOps.DELETE:
                self.del_items += load_batch

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
        # To provide little 'headroom' while loading/deleting docs in batches
        mem_buffer_gap = 10000
        low_wm_reached = False
        high_wm_reached = False
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
        while not low_wm_reached:
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

        load_batch = 1
        self.log.info("Loading docs till high_water_mark is reached")
        while not high_wm_reached:
            perform_doc_op(DocLoading.Bucket.DocOps.CREATE)
            stats = nodes_data[target_node]["cbstat"].all_stats(bucket.name)
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
        while not high_wm_reached:
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
        if SDKException.DocumentNotFoundException not in str(result["error"]):
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
        if SDKException.DocumentNotFoundException not in str(result["error"]):
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
        doc_gen = doc_generator(self.key, 0, self.num_items, doc_size=10)
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
        doc_gen_small = doc_generator(self.key, 0, 500, doc_size=10)
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
            self.bucket_util._wait_warmup_completed([target_nodes],
                                                    small_bucket)
            and (not self.bucket_util._wait_warmup_completed(
                 [target_nodes], big_bucket, self.warmup_timeout)),
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
                                doc_size=1,
                                target_vbucket=active_vbs)

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

        retry = 0
        before_stats = None
        warmup_running = False
        # Kill memcached during ttl load
        cb_error.create(CouchbaseError.KILL_MEMCACHED)
        while not warmup_running and retry < 10:
            try:
                warmup_stats = cb_stat.warmup_stats(bucket.name)
                self.log.info("Current warmup state %s:%s"
                              % (warmup_stats["ep_warmup_thread"],
                                 warmup_stats["ep_warmup_state"]))
                if warmup_stats["ep_warmup_thread"] != "complete":
                    warmup_running = True
                    while before_stats is None:
                        before_stats = cb_stat.vbucket_details(bucket.name)
                    self.log.info("Starting read task to trigger purger")
                    self.task_manager.add_new_task(load_task)
                    warmup_stats = cb_stat.warmup_stats(bucket.name)
                    cb_error.create(CouchbaseError.KILL_MEMCACHED)
                    self.log.info("Warmup state during mc_kill %s:%s"
                                  % (warmup_stats["ep_warmup_thread"],
                                     warmup_stats["ep_warmup_state"]))
                    if warmup_stats["ep_warmup_thread"] == "complete":
                        self.log_failure("Can't trust the outcome, "
                                         "bucket warmed_up before mc_kill")
                    self.task_manager.get_task_result(load_task)
            except Exception:
                pass
            finally:
                retry += 1
                self.sleep(0.3)
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
        key_vb = self.bucket_util.get_vbucket_num_for_key(key)
        bucket = self.cluster.buckets[0]
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
                                    target_vbucket=[key_vb])
            while doc_gen.has_next():
                d_key, val = doc_gen.next()
                client.crud(DocLoading.Bucket.DocOps.CREATE, d_key, val)

            output, _ = shell.execute_command(hash_dump_cmd)
            if not output:
                is_resident = False
            start_index = doc_gen.key_counter
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
        xdcr_rest = RestConnection(xdcr_cluster.master)
        xdcr_rest.init_node()
        xdcr_rest.set_internalSetting("magmaMinMemoryQuota", 256)
        self.create_bucket(xdcr_cluster)
        rest = RestConnection(self.cluster.master)
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
            q_str = urllib.quote_plus(
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
        total_docs = self.cluster.vbuckets * docs_per_vb
        doc_val = {"f": "test"}
        doc_keys = dict([(vb_num, list())
                        for vb_num in range(0, self.cluster.vbuckets)])
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
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(key)
            if vb_for_key in req_key_for_vb:
                doc_keys[vb_for_key].append(key)
                if len(doc_keys[vb_for_key]) == docs_per_vb:
                    req_key_for_vb.remove(vb_for_key)

        # Open SDK client for loading
        client = SDKClient(self.cluster, self.cluster.buckets[0])

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
               self.cluster.buckets[0].name, param, param_val)

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
        all_stats = cbstat.all_stats(self.cluster.buckets[0].name)
        curr_val = int(all_stats["ep_warmup_backfill_scan_chunk_duration"])
        self.assertEqual(curr_val, param_val,
                         "Unexpected value: '%s'" % curr_val)

        # Run memcached restart and validate the warmup and vb stats
        for index in range(1, 10):
            self.log.info("Running iteration: %s" % index)
            cb_err.create(CouchbaseError.KILL_MEMCACHED)

            self.log.info("Wait for warmup to complete")
            self.bucket_util.is_warmup_complete(self.cluster.buckets)

            warmup_stats = cbstat.all_stats(self.cluster.buckets[0].name,
                                            "warmup")
            for key in ["ep_warmup_estimated_key_count",
                        "ep_warmup_estimated_value_count",
                        "ep_warmup_key_count",
                        "ep_warmup_value_count"]:
                self.assertFalse(int(warmup_stats[key]) != total_docs,
                                 "Value mismatch. %s = %s"
                                 % (key, warmup_stats[key]))

            vb_details = cbstat.vbucket_details(self.cluster.buckets[0].name)
            for vb_num, vb_stats in vb_details.items():
                self.assertFalse(int(vb_stats["num_items"]) != docs_per_vb,
                                 "Vb %s reports less num_items: %s"
                                 % (vb_num, vb_stats["num_items"]))

        param_val = 100
        self.log.info("Reset the value back to %s" % param_val)
        diag_eval_cmd = diag_eval_cmd_format \
            % (self.cluster.master.rest_username,
               self.cluster.master.rest_password,
               self.cluster.buckets[0].name, param, param_val)

        self.log.info("Running diag_eval and restarting couchbase-server")
        self.log.info(diag_eval_cmd)
        shell.execute_command(diag_eval_cmd)
        self.sleep(5, "Wait before restarting the cluster")
        shell.restart_couchbase()

        self.log.info("Wait for warmup to complete")
        self.bucket_util.is_warmup_complete(self.cluster.buckets)

        all_stats = cbstat.all_stats(self.cluster.buckets[0].name)
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
                                   vbuckets=self.cluster.vbuckets,
                                   randomize_doc_size=self.randomize_doc_size,
                                   randomize_value=self.randomize_value)
        gen_create2 = doc_generator("eviction2_",
                                    start=0,
                                    end=self.num_items,
                                    key_size=self.key_size,
                                    doc_size=self.doc_size,
                                    doc_type=self.doc_type,
                                    vbuckets=self.cluster.vbuckets,
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
                                       RestConnection(self.cluster.master))
        except Exception as e:
            if "User was not found." not in str(e):
                raise e

        self.log.info("Creating user '%s' with data_reader persmission" % user)
        rbac_util.create_user_source(testuser, 'builtin', self.cluster.master)
        status = rbac_util.add_user_role(
            rolelist, RestConnection(self.cluster.master), 'builtin')
        self.assertEqual(status[0]["id"], user, "User create failed")

        key = "test"
        bucket = self.cluster.buckets[0]
        client = SDKClient(self.cluster, bucket)
        insert_option = SDKOptions.get_insert_options()

        client.collection.insert(key, "null", insert_option.transcoder(
            RawJsonTranscoder.INSTANCE))
        client.crud(DocLoading.Bucket.SubDocOps.INSERT, key,
                    ["_xattr", "test_val"], xattr=True)
        client.close()

        client = SDKClient(self.cluster, bucket, username=user)
        result = client.crud(DocLoading.Bucket.SubDocOps.LOOKUP, key,
                             "$XTOC", xattr=True)
        client.close()
        result = str(result[0][key]['value'])
        self.assertEqual('[[]]', result, "Value mismatch: %s" % result)

    def test_defragmenter_sleep_time(self):
        """
        Ref: MB-55943
        """
        result = True
        bucket = self.cluster.buckets[0]
        iterations = self.input.param("iterations", 2000)
        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)

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
        cb_stats = Cbstats(shell)

        non_ttl_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen,
            DocLoading.Bucket.DocOps.CREATE, batch_size=1000,
            process_concurrency=2, print_ops_rate=False,
            skip_read_on_error=True, suppress_error_table=True,
            sdk_client_pool=self.sdk_client_pool, iterations=-1,
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
                    sdk_client_pool=self.sdk_client_pool, iterations=3,
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
        rest = RestConnection(self.cluster.master)
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

        self.log.info("Creating GSI index on collection 'c1'")
        client = SDKClient(self.cluster, bucket)
        _ = client.run_query(
            "CREATE INDEX `c1` ON `{}`.`_default`.`c1`(body) USING GSI"
            .format(bucket.name), timeout=300)
        query = "SELECT state FROM system:indexes WHERE name='c1'"
        state = client.cluster.query(query).rowsAsObject()[0].get("state")
        client.close()
        if state != "online":
            self.fail("Create index timed out")
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
        Ref: MB-58088 / MB-59060 / MB-59746
        """
        def validate_unlock_exception(t_key, d_cas, expected_errors):
            try:
                client.collection.unlock(t_key, d_cas)
            except AmbiguousTimeoutException as e:
                if "KV_TEMPORARY_FAILURE" in str(e):
                    self.fail("Key '{}' - KV_TEMPORARY_FAILURE".format(t_key))
            except CouchbaseException as e:
                err_found = False
                for exp_err in expected_errors:
                    if exp_err in str(e):
                        err_found = True
                        break
                self.assertTrue(err_found,
                                f"Key '{t_key}' - Unexpected error: {e}")

        key_1 = "test_doc_1"
        key_2 = "test_doc_2"
        key_3 = "test_doc_3"
        bucket = self.cluster.buckets[0]
        if self.cluster.sdk_client_pool:
            client = self.cluster.sdk_client_pool.get_client_for_bucket(bucket)
        else:
            client = SDKClient(self.cluster, bucket)

        not_locked_msgs = ["Requested resource is not locked", "NOT_LOCKED"]
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
        cas = client.crud(DocLoading.Bucket.DocOps.UPDATE, key_2, {})["cas"]
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
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
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
        client = self.cluster.sdk_client_pool.get_client_for_bucket(bucket)
        vb_for_key = self.bucket_util.get_vbucket_num_for_key(
            key, self.cluster.vbuckets)

        self.log.info("Disabling auto-failover settings")
        RestConnection(self.cluster.master)\
            .update_autofailover_settings(False, 60)

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
                                doc_size=1024, target_vbucket=target_vbs)

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
        self.cluster.sdk_client_pool.release_client(client)
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
            active_rr_perc = int(stats["vb_active_perc_mem_resident"])
            replica_rr_perc = int(stats["vb_replica_perc_mem_resident"])
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
            active_rr_perc = int(stats["vb_active_perc_mem_resident"])
            replica_rr_perc = int(stats["vb_replica_perc_mem_resident"])
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
                self.fail("<MemcachedError #%d ``%s''>"
                          % (error.status, error.message))
            if count % 1000 == 0:
                self.log.info('The number of iteration is {}'.format(count))
