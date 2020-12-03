import json
from random import choice, randint
from threading import Thread

from BucketLib.BucketOperations import BucketHelper
from basetestcase import BaseTestCase
from Cb_constants import constants, CbServer, DocLoading
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from cb_tools.mc_stat import McStat
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from error_simulation.cb_error import CouchbaseError

from mc_bin_client import MemcachedClient, MemcachedError
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
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


class basic_ops(BaseTestCase):
    def setUp(self):
        super(basic_ops, self).setUp()

        self.doc_ops = self.input.param("doc_ops", "").split(";")
        self.observe_test = self.input.param("observe_test", False)
        # Scope/collection name can be default or create a random one to test
        self.scope_name = self.input.param("scope", CbServer.default_scope)
        self.collection_name = self.input.param("collection",
                                                CbServer.default_collection)

        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode,
            bucket_type=self.bucket_type, storage=self.bucket_storage,
            ram_quota=self.bucket_size,
            eviction_policy=self.bucket_eviction_policy)
        self.bucket_util.add_rbac_user()

        # Create Scope/Collection with random names if not equal to default
        if self.scope_name != CbServer.default_scope:
            self.scope_name = self.bucket_util.get_random_name()
            self.bucket_util.create_scope(self.cluster.master,
                                          self.bucket_util.buckets[0],
                                          {"name": self.scope_name})
        if self.collection_name != CbServer.default_collection:
            self.collection_name = self.bucket_util.get_random_name()
            self.bucket_util.create_collection(self.cluster.master,
                                               self.bucket_util.buckets[0],
                                               self.scope_name,
                                               {"name": self.collection_name,
                                                "num_items": self.num_items})
            self.log.info("Using scope::collection - '%s::%s'"
                          % (self.scope_name, self.collection_name))

        # Update required num_items under default collection
        self.bucket_util.buckets[0] \
            .scopes[self.scope_name] \
            .collections[self.collection_name] \
            .num_items = self.num_items

        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            durability=self.durability_level,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to)

        # Create sdk_clients for pool
        if self.sdk_client_pool:
            self.log.info("Creating SDK client pool")
            self.sdk_client_pool.create_clients(
                self.bucket_util.buckets[0],
                self.cluster.nodes_in_cluster,
                req_clients=self.sdk_pool_capacity,
                compression_settings=self.sdk_compression)

        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
        self.log.info("==========Finished Basic_ops base setup========")

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def do_basic_ops(self):
        KEY_NAME = 'key1'
        KEY_NAME2 = 'key2'
        self.log.info('Starting basic ops')

        default_bucket = self.bucket_util.get_all_buckets()[0]
        sdk_client = SDKClient([self.cluster.master],
                               default_bucket,
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
                             vbuckets=self.cluster_util.vbuckets,
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
        def_bucket = self.bucket_util.buckets[0]
        ignore_exceptions = list()
        retry_exceptions = list()
        supported_d_levels = self.bucket_util.get_supported_durability_levels()

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
            vbuckets=self.cluster_util.vbuckets,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value)
        self.log.info("Loading {0} docs into the bucket: {1}"
                      .format(self.num_items, def_bucket))
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, doc_create, "create", 0,
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
            sdk_client_pool=self.sdk_client_pool)
        self.task.jython_task_manager.get_task_result(task)

        if self.ryow:
            check_durability_failures()

        # Retry doc_exception code
        self.log.info("Validating failed doc's (if any) exceptions")
        doc_op_info_dict = dict()
        doc_op_info_dict[task] = self.bucket_util.get_doc_op_info_dict(
            def_bucket, "create", exp=0, replicate_to=self.replicate_to,
            persist_to=self.persist_to, durability=self.durability_level,
            timeout=self.sdk_timeout, time_unit="seconds",
            ignore_exceptions=ignore_exceptions,
            retry_exceptions=retry_exceptions)
        self.bucket_util.verify_doc_op_task_exceptions(doc_op_info_dict,
                                                       self.cluster,
                                                       self.sdk_client_pool)

        if len(doc_op_info_dict[task]["unwanted"]["fail"].keys()) != 0:
            self.fail("Failures in retry doc CRUDs: {0}"
                      .format(doc_op_info_dict[task]["unwanted"]["fail"]))

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets()

        # Update ref_val
        verification_dict["ops_create"] += \
            self.num_items - len(task.fail.keys())
        # Validate vbucket stats
        if self.durability_level in supported_d_levels:
            verification_dict["sync_write_committed_count"] += self.num_items

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        # Verify initial doc load count
        self.log.info("Validating doc_count in buckets")
        self.bucket_util.validate_doc_count_as_per_collections(def_bucket)

        self.log.info("Creating doc_generator for doc_op")
        num_item_start_for_crud = int(self.num_items / 2)
        doc_update = doc_generator(
            self.key, 0, num_item_start_for_crud,
            key_size=self.key_size,
            doc_size=self.doc_size, doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets,
            mutate=1,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value)

        if self.target_vbucket:
            mutation_doc_count = len(doc_update.doc_keys)
        else:
            mutation_doc_count = (doc_update.end - doc_update.start
                                  + len(task.fail.keys()))

        if doc_op == "update":
            self.log.info("Performing 'update' mutation over the docs")
            task = self.task.async_load_gen_docs(
                self.cluster, def_bucket, doc_update, "update", 0,
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
                sdk_client_pool=self.sdk_client_pool)
            self.task.jython_task_manager.get_task_result(task)
            verification_dict["ops_update"] += mutation_doc_count
            if self.durability_level in supported_d_levels:
                verification_dict["sync_write_committed_count"] \
                    += mutation_doc_count
            if self.ryow:
                check_durability_failures()

            # Read all the values to validate update operation
            task = self.task.async_validate_docs(
                    self.cluster, def_bucket,
                    doc_update, "update", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    scope=self.scope_name,
                    collection=self.collection_name,
                    sdk_client_pool=self.sdk_client_pool)
            self.task.jython_task_manager.get_task_result(task)

        elif doc_op == "delete":
            self.log.info("Performing 'delete' mutation over the docs")
            task = self.task.async_load_gen_docs(
                self.cluster, def_bucket, doc_update, "delete", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                ryow=self.ryow, check_persistence=self.check_persistence,
                scope=self.scope_name,
                collection=self.collection_name,
                sdk_client_pool=self.sdk_client_pool)
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
                    self.cluster, def_bucket,
                    doc_update, "delete", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    sdk_client_pool=self.sdk_client_pool)
            self.task.jython_task_manager.get_task_result(task)

        else:
            self.log.warning("Unsupported doc_operation")

        self.log.info("Wait for ep_all_items_remaining to become '0'")
        self.bucket_util._wait_for_stats_all_buckets()

        failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.fail("Cbstat vbucket-details verification failed")

        self.log.info("Validating doc_count")
        self.bucket_util.validate_doc_count_as_per_collections(def_bucket)

    def test_large_doc_size(self):
        # bucket size=256MB, when Bucket gets filled 236MB then
        # test starts failing document size=2MB, No of docs = 221,
        # load 250 docs generate docs with size >= 1MB , See MB-29333

        self.doc_size *= 1024*1024
        gens_load = self.generate_docs_bigdata(
            docs_per_day=self.num_items, document_size=self.doc_size)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gens_load, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                sdk_client_pool=self.sdk_client_pool)
            self.task.jython_task_manager.get_task_result(task)

        # check if all the documents(250) are loaded with default timeout
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_large_doc_20MB(self):
        # test reproducer for MB-29258,
        # Load a doc which is greater than 20MB
        # with compression enabled and check if it fails
        # check with compression_mode as active, passive and off
        val_error = SDKException.ValueTooLargeException
        gens_load = self.generate_docs_bigdata(
            docs_per_day=1, document_size=(self.doc_size * 1024000))
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gens_load, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout,
                sdk_client_pool=self.sdk_client_pool)
            self.task.jython_task_manager.get_task_result(task)
            if self.doc_size > 20:
                if len(task.fail.keys()) == 0:
                    self.log_failure("No failures during large doc insert")
                for doc_id, doc_result in task.fail.items():
                    if val_error not in str(doc_result["error"]):
                        self.log_failure("Invalid exception for key %s: %s"
                                         % (doc_id, doc_result))
            else:
                if len(task.fail.keys()) != 0:
                    self.log_failure("Failures during large doc insert")

        for bucket in self.bucket_util.buckets:
            if self.doc_size > 20:
                # failed with error "Data Too Big" when document size > 20MB
                self.bucket_util.verify_stats_all_buckets(0)
            else:
                self.bucket_util.verify_stats_all_buckets(1)
                gens_update = self.generate_docs_bigdata(
                    docs_per_day=1, document_size=(21 * 1024000))
                task = self.task.async_load_gen_docs(
                    self.cluster, bucket, gens_update, "update", 0,
                    batch_size=10,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    durability=self.durability_level,
                    compression=self.sdk_compression,
                    timeout_secs=self.sdk_timeout,
                    sdk_client_pool=self.sdk_client_pool)
                self.task.jython_task_manager.get_task_result(task)
                if len(task.fail.keys()) != 1:
                    self.log_failure("Large docs inserted for keys: %s"
                                     % task.fail.keys())
                if len(task.fail.keys()) == 0:
                    self.log_failure("No failures during large doc insert")
                for key, crud_result in task.fail.items():
                    if SDKException.ValueTooLargeException \
                            not in str(crud_result["error"]):
                        self.log_failure("Unexpected error for key %s: %s"
                                         % (key, crud_result["error"]))
                for doc_id, doc_result in task.fail.items():
                    if val_error not in str(doc_result["error"]):
                        self.log_failure("Invalid exception for key %s: %s"
                                         % (doc_id, doc_result))
                self.bucket_util.verify_stats_all_buckets(1)
        self.validate_test_failure()

    def test_parallel_cruds(self):
        data_op_dict = dict()
        num_items = self.num_items
        half_of_num_items = self.num_items / 2
        supported_d_levels = self.bucket_util.get_supported_durability_levels()
        exp_values_to_test = [0, 900, 4000, 12999]

        # Initial doc_loading
        initial_load = doc_generator(self.key, 0, self.num_items,
                                     doc_size=self.doc_size)
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket_util.buckets[0], initial_load,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=100, process_concurrency=8,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool)
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
            if self.num_replicas > 0:
                replicate_to = randint(1, self.num_replicas)
                persist_to = randint(0, self.num_replicas + 1)
            if not self.observe_test and choice([True, False]):
                d_level = choice(supported_d_levels)

            self.log.info("Doc_op %s, range (%d, %d), "
                          "replicate_to=%s, persist_to=%s, d_level=%s"
                          % (doc_op, gen_start, gen_end,
                             replicate_to, persist_to, d_level))

            data_op_dict[doc_op] = dict()
            data_op_dict[doc_op]["doc_gen"] = doc_generator(
                self.key, gen_start, gen_end,
                doc_size=self.doc_size,
                mutation_type=doc_op)
            data_op_dict[doc_op]["task"] = self.task.async_load_gen_docs(
                self.cluster, self.bucket_util.buckets[0],
                data_op_dict[doc_op]["doc_gen"], doc_op,
                exp=choice(exp_values_to_test),
                compression=self.sdk_compression,
                persist_to=persist_to, replicate_to=replicate_to,
                durability=d_level, timeout_secs=self.sdk_timeout,
                sdk_client_pool=self.sdk_client_pool,
                process_concurrency=1, batch_size=1,
                print_ops_rate=False, start_task=False,
                task_identifier="%s_%d" % (doc_op, op_index))

        # Start all tasks
        for doc_op in self.doc_ops:
            self.task_manager.add_new_task(data_op_dict[doc_op]["task"])
        # Wait for doc_ops to complete and validate final doc value result
        for doc_op in self.doc_ops:
            self.task_manager.get_task_result(data_op_dict[doc_op]["task"])
            self.log.info("%s task completed" % doc_op)
            if data_op_dict[doc_op]["task"].fail:
                self.log_failure("Doc_loading failed for %s: %s"
                                 % (doc_op, data_op_dict[doc_op]["task"].fail))
            elif doc_op in [DocLoading.Bucket.DocOps.CREATE,
                            DocLoading.Bucket.DocOps.UPDATE,
                            DocLoading.Bucket.DocOps.REPLACE,
                            DocLoading.Bucket.DocOps.DELETE]:
                suppress_err_tbl = False
                if doc_op == DocLoading.Bucket.DocOps.DELETE:
                    suppress_err_tbl = True
                self.log.info("Validating %s results" % doc_op)
                # Read all the values to validate doc_operation values
                task = self.task.async_validate_docs(
                    self.cluster, self.bucket_util.buckets[0],
                    data_op_dict[doc_op]["doc_gen"], doc_op, 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    sdk_client_pool=self.sdk_client_pool,
                    suppress_error_table=suppress_err_tbl)
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
        command = cmd_base + '-X POST -d \'case file:read_file("/etc/passwd") of {ok, B} -> io:format("~p~n", [binary_to_term(B)]) end.\''
        cmd.append(command)

        shell = RemoteMachineShellConnection(self.cluster.master)
        for command in cmd:
            output, error = shell.execute_command(command)
            self.assertNotEquals("API is accessible from localhost only", output[0])

        # Disable allow_nonlocal_eval
        if not self.disable_diag_eval_on_non_local_host:
            command = cmd_base + '-X POST -d \'ns_config:set(allow_nonlocal_eval, true).\''
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
        command = cmd_base + '-X POST -d \'case file:read_file("/etc/passwd") of {ok, B} -> io:format("~p~n", [binary_to_term(B)]) end.\''
        cmd.append(command)

        for command in cmd:
            output, error = shell.execute_command(command)
            if self.disable_diag_eval_on_non_local_host:
                self.assertEquals("API is accessible from localhost only",
                                  output[0])
            else:
                self.assertNotEquals("API is accessible from localhost only",
                                     output[0])

    def test_MB_40967(self):
        """
        1. Load initial docs into the bucket
        2. Perform continuous reads until get_cmd stats breaks in
           'cbstats timings' command
        """
        total_gets = 0
        max_gets = 2500000000
        bucket = self.bucket_util.buckets[0]
        doc_gen = doc_generator(self.key, 0, self.num_items,
                                doc_size=1)
        create_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, "create", 0,
            batch_size=100,
            process_concurrency=self.process_concurrency,
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(create_task)

        cbstat = dict()
        kv_nodes = self.cluster_util.get_kv_nodes()
        for node in kv_nodes:
            shell = RemoteMachineShellConnection(node)
            cbstat[node] = Cbstats(shell)

        self.log.info("Start doc_reads until total_gets cross: %s" % max_gets)
        read_task = self.task.async_continuous_doc_ops(
            self.cluster, bucket, doc_gen,
            op_type="read", batch_size=self.batch_size,
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
                    total_gets += long(vb_stats["ops_get"])
            if self.test_failure:
                break
            self.sleep(120, "Total_gets: %s, itr: %s" % (total_gets,
                                                         read_task.itr_count))

        read_task.end_task()
        self.task_manager.get_task_result(read_task)

        # Close all shell connections
        for node in kv_nodes:
            cbstat[node].shellConn.disconnect()

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
                for t_node in mc_stat.keys():
                    try:
                        mc_stat[t_node].reset(bucket_name)
                    except Exception as mcstat_err:
                        self.log_failure(mcstat_err)
                if self.test_failure:
                    break

            for t_node in mc_stat.keys():
                mc_stat[t_node].shellConn.disconnect()

        def get_timings(bucket_name):
            cb_stat = dict()
            for t_node in kv_nodes:
                shell_conn = RemoteMachineShellConnection(t_node)
                cb_stat[t_node] = Cbstats(shell_conn)

            while not stop_thread:
                for t_node in cb_stat.keys():
                    try:
                        cb_stat[t_node].get_timings(bucket_name)
                    except Exception as cbstat_err:
                        self.log_failure(cbstat_err)
                if self.test_failure:
                    break

            for t_node in cb_stat.keys():
                cb_stat[t_node].shellConn.disconnect()

        total_gets = 0
        max_gets = 50000000
        stop_thread = False
        bucket = self.bucket_util.buckets[0]
        cb_stat_obj = dict()
        kv_nodes = self.cluster_util.get_kv_nodes()
        for node in self.cluster_util.get_kv_nodes():
            shell = RemoteMachineShellConnection(node)
            cb_stat_obj[node] = Cbstats(shell)

        doc_gen = doc_generator(self.key, 0, self.num_items, doc_size=1)
        create_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, "create", 0,
            batch_size=500,
            process_concurrency=self.process_concurrency,
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(create_task)

        mc_stat_reset_thread = Thread(target=reset_mcstat,
                                      args=[bucket.name])
        get_timings_thread = Thread(target=get_timings,
                                    args=[bucket.name])
        mc_stat_reset_thread.start()
        get_timings_thread.start()

        read_task = self.task.async_continuous_doc_ops(
            self.cluster, bucket, doc_gen,
            op_type="read", batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            timeout_secs=self.sdk_timeout)

        while total_gets < max_gets:
            total_gets = 0
            try:
                for node in cb_stat_obj.keys():
                    vb_details = cb_stat_obj[node].vbucket_details(bucket.name)
                    for _, vb_stats in vb_details.items():
                        total_gets += long(vb_stats["ops_get"])
            except Exception as err:
                self.log_failure(err)

            self.log.info("Total gets: %s" % total_gets)
            result, core_msg, stream_msg, asan_msg = self.check_coredump_exist(
                self.servers, force_collect=True)

            if result is not False:
                self.log_failure(core_msg + stream_msg + asan_msg)
                break
            elif self.test_failure:
                break

            self.sleep(60, "Wait before next check")

        stop_thread = True
        read_task.end_task()
        mc_stat_reset_thread.join()
        get_timings_thread.join()

        # Close all shell connections
        for node in cb_stat_obj.keys():
            cb_stat_obj[node].shellConn.disconnect()

        self.validate_test_failure()

    def test_MB_41255(self):
        def create_docs_with_xattr():
            value = {'val': 'a' * self.doc_size}
            xattr_kv = ["field", "value"]
            while not stop_loader:
                t_key = "%s-%s" % (self.key, self.num_items)
                crud_result = client.crud("create", t_key, value,
                                          timeout=60)
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
        bucket = self.bucket_util.buckets[0]

        for node in self.cluster_util.get_kv_nodes():
            nodes_data[node] = dict()
            nodes_data[node]["shell"] = RemoteMachineShellConnection(node)
            nodes_data[node]["cbstats"] = Cbstats(nodes_data[node]["shell"])
            nodes_data[node]["active_vbs"] = nodes_data[node][
                "cbstats"].vbucket_list(bucket.name, "active")
            nodes_data[node]["replica_vbs"] = nodes_data[node][
                "cbstats"].vbucket_list(bucket.name, "replica")

        bucket_helper = BucketHelper(self.cluster.master)
        client = SDKClient([self.cluster.master], bucket)

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
        self.bucket_util._wait_for_stats_all_buckets()

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

        self.log.info("Non-resident key count: %d" % non_resident_keys_len)

        # Start rebalance-out operation
        rebalance_out = self.task.async_rebalance(
            self.cluster.servers[0:self.nodes_init], [],
            [self.cluster.servers[-1]])
        self.sleep(10, "Wait for rebalance to start")

        # Start deleting the evicted docs in parallel to rebalance task
        self.log.info("Deleting evicted keys")
        for key in non_resident_keys:
            result = client.crud("delete", key)
            if result["status"] is False:
                self.log_failure("Key %s deletion failed: %s"
                                 % (key, result["error"]))

        # Wait for rebalance to complete
        self.task_manager.get_task_result(rebalance_out)

        # Wait for ep_engine_queue size to become '0'
        self.bucket_util._wait_for_stats_all_buckets()

        # Trigger compaction
        self.bucket_util._run_compaction(number_of_times=1)

        # Read all deleted keys (include replica read) to validate
        for key in non_resident_keys:
            result = client.get_from_all_replicas(key)
            if result:
                self.log_failure("Key '%s' exists on %d replica(s)"
                                 % (key, len(result)))

        # Close SDK and shell connections
        client.close()
        for node in nodes_data.keys():
            nodes_data[node]["shell"].disconnect()

        self.assertTrue(rebalance_out.result, "Rebalance_out failed")

        self.bucket_util.verify_stats_all_buckets(self.num_items
                                                  - non_resident_keys_len)
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
        bucket = self.bucket_util.buckets[0]
        target_vb = choice(range(self.cluster_util.vbuckets))
        vb_str = str(target_vb)
        doc_gen = doc_generator(self.key, 0, self.num_items,
                                target_vbucket=[target_vb])

        target_node = None
        nodes_data = dict()
        for node in self.cluster_util.get_kv_nodes():
            nodes_data[node] = dict()
            nodes_data[node]["shell"] = RemoteMachineShellConnection(node)
            nodes_data[node]["cbstats"] = Cbstats(nodes_data[node]["shell"])
            nodes_data[node]["active_vbs"] = nodes_data[node][
                "cbstats"].vbucket_list(bucket.name, "active")
            if target_vb in nodes_data[node]["active_vbs"]:
                target_node = node

        # Open SDK client for doc_ops
        client = SDKClient([self.cluster.master], bucket)

        self.log.info("Testing using vbucket %s" % target_vb)
        while doc_gen.has_next():
            key, val = doc_gen.next()
            vb_for_key = self.bucket_util.get_vbucket_num_for_key(key)

            # Create and delete a key
            result = client.crud("create", key, val)
            validate_crud_result("create", key, result)
            result = client.crud("delete", key, val)
            validate_crud_result("delete", key, result)
            on_disk_deletes += 1

            # Wait for ep_queue_size to become zero
            self.bucket_util._wait_for_stats_all_buckets()

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

                self.bucket_util._run_compaction(number_of_times=1)

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
            client.crud("create", key, val)
            client.crud("delete", key, val)
            # self.bucket_util._wait_for_stats_all_buckets()

        self.bucket_util._run_compaction(number_of_times=1)
        self.sleep(5, "Compaction complete")
        vb_details_stats = nodes_data[target_node][
            "cbstats"].vbucket_details(bucket.name)
        bloom_filter_size_after_compaction = \
            vb_details_stats[vb_str]["bloom_filter_size"]
        self.log.info("Bloom filter size after compaction: %s"
                      % bloom_filter_size_after_compaction)
        if int(bloom_filter_size_after_compaction) <= int(bloom_filter_size):
            self.log_failure("Bloom filter init_size <= curr_size")

        # Close SDK and shell connections
        client.close()
        for node in nodes_data.keys():
            nodes_data[node]["shell"].disconnect()

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
            if op_type == "delete":
                start = self.del_items
            doc_gen = doc_generator(self.key, start, start+load_batch,
                                    doc_size=self.doc_size)
            doc_op_task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_gen, op_type,
                timeout_secs=self.sdk_timeout,
                print_ops_rate=False,
                skip_read_on_error=True,
                suppress_error_table=True,
                batch_size=100,
                process_concurrency=8)
            self.task_manager.get_task_result(doc_op_task)
            self.bucket_util._wait_for_stats_all_buckets()
            if op_type == "create":
                self.num_items += load_batch
            elif op_type == "delete":
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
        load_batch = 20000
        low_wm_reached = False
        high_wm_reached = False
        wm_tbl = TableView(self.log.info)
        bucket = self.bucket_util.buckets[0]

        wm_tbl.set_headers(["Stat", "Memory Val", "Percent"])
        kv_nodes = self.cluster_util.get_kv_nodes()

        for node in kv_nodes:
            shell = RemoteMachineShellConnection(node)
            nodes_data[node] = dict()
            nodes_data[node]["shell"] = shell
            nodes_data[node]["cbstat"] = Cbstats(shell)
            nodes_data[node]["eviction_start"] = False
            nodes_data[node]["active_vbs"] = nodes_data[node][
                "cbstat"].vbucket_list(bucket.name, "active")
            nodes_data[node]["replica_vbs"] = nodes_data[node][
                "cbstat"].vbucket_list(bucket.name, "replica")

        target_node = choice(kv_nodes)
        cbepctl = Cbepctl(nodes_data[target_node]["shell"])

        self.log.info("Loading till low_water_mark is reached")
        while not low_wm_reached:
            perform_doc_op("create")
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

        self.log.info("Loading docs till high_water_mark is reached")
        while not high_wm_reached:
            perform_doc_op("create")
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
            perform_doc_op("delete")
            stats = nodes_data[target_node]["cbstat"].all_stats(bucket.name)
            if int(stats["mem_used"]) < int(stats["ep_mem_low_wat"]):
                low_wm_reached = True
                display_bucket_water_mark_values(target_node)
                self.log.info("Low water_mark reached")

        if int(stats["ep_num_pager_runs"]) != 0:
            self.log_failure("ItemPager ran after del_op & non_io_threads=0")

        self.log.info("Setting num_nonio_threads=8")
        cbepctl.set(bucket.name, "flush_param", "num_nonio_threads", 8)

        retry_count = 0
        while retry_count < 10:
            retry_count += 1
            stats = display_bucket_water_mark_values(target_node)
            if int(stats["ep_num_pager_runs"]) == 1:
                break
            self.sleep(1, "ep_num_pager_runs actual::0, expected::1. "
                          " Will retry..")
        else:
            self.log_failure("ItemPager not run with lower_wm levels")

        self.log.info("Loading docs till high_water_mark is reached")
        high_wm_reached = False
        while not high_wm_reached:
            perform_doc_op("create")
            stats = nodes_data[target_node]["cbstat"].all_stats(bucket.name)
            if int(stats["mem_used"]) > int(stats["ep_mem_high_wat"]):
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
        for node in nodes_data.keys():
            nodes_data[node]["shell"].disconnect()

        self.validate_test_failure()

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
                                   vbuckets=self.cluster_util.vbuckets,
                                   randomize_doc_size=self.randomize_doc_size,
                                   randomize_value=self.randomize_value)
        gen_create2 = doc_generator("eviction2_",
                                    start=0,
                                    end=self.num_items,
                                    key_size=self.key_size,
                                    doc_size=self.doc_size,
                                    doc_type=self.doc_type,
                                    vbuckets=self.cluster_util.vbuckets,
                                    randomize_doc_size=self.randomize_doc_size,
                                    randomize_value=self.randomize_value)
        def_bucket = self.bucket_util.get_all_buckets()[0]
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        remote = RemoteMachineShellConnection(self.cluster.master)
        for bucket in self.bucket_util.buckets:
            # change compression mode to off
            output, _ = remote.execute_couchbase_cli(
                cli_command='bucket-edit', cluster_host="localhost:8091",
                user=self.cluster.master.rest_username,
                password=self.cluster.master.rest_password,
                options='--bucket=%s --compression-mode off' % bucket.name)
            self.assertTrue(' '.join(output).find('SUCCESS') != -1,
                            'compression mode set to off')

            # sleep for 10 sec (minimum 250sec)
            self.sleep(10)

        # Load data and check stats to see compression
        # is not done for newly added data
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create2, "create", 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items*2)

    def MB36948(self):
        node_to_stop = self.servers[0]
        self.log.info("Adding index/query node")
        self.task.rebalance([self.cluster.master], [self.servers[2]], [],
                            services=["n1ql,index"])
        self.log.info("Creating SDK client connection")
        client = SDKClient([self.cluster.master],
                           self.bucket_util.buckets[0],
                           compression_settings=self.sdk_compression)

        self.log.info("Stopping memcached on: %s" % node_to_stop)
        ssh_conn = RemoteMachineShellConnection(node_to_stop)
        err_sim = CouchbaseError(self.log, ssh_conn)
        err_sim.create(CouchbaseError.STOP_MEMCACHED)

        result = client.crud("create", "abort1", "abort1_val")
        if not result["status"]:
            self.log_failure("Async SET failed")

        result = client.crud("update", "abort1", "abort1_val",
                             durability=self.durability_level,
                             timeout=3, time_unit="seconds")
        if result["status"]:
            self.log_failure("Sync write succeeded")
        if SDKException.DurabilityAmbiguousException not in result["error"]:
            self.log_failure("Invalid exception for sync_write: %s" % result)

        self.log.info("Resuming memcached on: %s" % node_to_stop)
        err_sim.revert(CouchbaseError.STOP_MEMCACHED)

        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(1)

        self.log.info("Closing ssh & SDK connections")
        ssh_conn.disconnect()
        client.close()

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
                self.fail("<MemcachedError #%d ``%s''>"
                          % (error.status, error.message))
            if count % 1000 == 0:
                self.log.info('The number of iteration is {}'.format(count))
