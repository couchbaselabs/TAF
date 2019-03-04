import time
import json
from math import floor

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator, doc_generator
from couchbase_helper.tuq_generators import JsonGenerator

from membase.api.rest_client import RestConnection
# from memcached.helper.data_helper import VBucketAwareMemcached
from sdk_client import SDKSmartClient as VBucketAwareMemcached
from mc_bin_client import MemcachedClient, MemcachedError
from remote.remote_util import RemoteMachineShellConnection

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

        self.key = 'test_docs'.rjust(self.key_size, '0')

        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket(replica=self.num_replicas,
                                               compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()

        # self.src_bucket = RestConnection(self.cluster.master).get_buckets()
        self.src_bucket = self.bucket_util.get_all_buckets()
        # Reset active_resident_threshold to avoid further data load as DGM
        self.active_resident_threshold = 0
        self.log.info("==========Finished Basic_ops base setup========")

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def do_basic_ops(self):
        KEY_NAME = 'key1'
        KEY_NAME2 = 'key2'
        self.log.info('Starting basic ops')

        rest = RestConnection(self.cluster.master)
        default_bucket = self.bucket_util.get_all_buckets()[0]
        smart_client = VBucketAwareMemcached(rest, default_bucket)
        sdk_client = smart_client.get_client()
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
            opaque, rep_time, persist_time, persisted, cas = sdk_client.observe(KEY_NAME)

        try:
            rc = sdk_client.evict_key(KEY_NAME)
        except MemcachedError as exp:
            self.fail("Exception with evict meta - {0}".format(exp) )

        CAS = 0xabcd
        try:
            # key, exp, flags, seqno, cas
            rc = mcd.del_with_meta(KEY_NAME2, 0, 0, 2, CAS)
        except MemcachedError as exp:
            self.fail("Exception with del_with meta - {0}".format(exp))

    # Reproduce test case for MB-28078
    def do_setWithMeta_twice(self):
        mc = MemcachedClient(self.cluster.master.ip, 11210)
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
        self.log.info("Sleeping for 5 and checking stats again")
        time.sleep(5)
        stats = mc.stats()
        self.log.info('curr_items: {0} and curr_temp_items:{1}'
                      .format(stats['curr_items'], stats['curr_temp_items']))

        try:
            mc.setWithMeta('1', '{"Hello":"World"}', 3600, 0, 1,
                           0x1512a3186faa0000)
        except MemcachedError as error:
            stats = mc.stats()
            self.log.info('After 2nd setWithMeta(), curr_items: {} and curr_temp_items:{}'
                          .format(stats['curr_items'], stats['curr_temp_items']))
            if int(stats['curr_temp_items']) == 1:
                self.fail("Error on second setWithMeta(), expected curr_temp_items to be 0")
            else:
                self.log.info("<MemcachedError #%d ``%s''>"
                              % (error.status, error.message))

    def generate_docs_bigdata(self, docs_per_day, start=0,
                              document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(start=start,
                                                    end=docs_per_day,
                                                    value_size=document_size)

    def test_doc_size(self):
        """
        Basic tests for document CRUD operations using JSON docs
        """
        doc_op = self.input.param("doc_op", "")
        def_bucket = self.bucket_util.buckets[0]

        if doc_op == "":
            doc_op = None

        self.log.info("Creating doc_generator..")
        # Load basic docs into bucket
        doc_create = doc_generator(self.key, 0, self.num_items,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   target_vbucket=self.target_vbucket,
                                   vbuckets=self.vbuckets)
        self.log.info("doc_generator created")
        print_ops_task = self.bucket_util.async_print_bucket_ops(def_bucket)
        task = self.task.async_load_gen_docs(self.cluster, def_bucket,
                                             doc_create, "create", 0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to,
                                             timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)
        print_ops_task.end_task()
        self.task_manager.get_task_result(print_ops_task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        num_item_start_for_crud = int(floor(self.num_items / 2)) + 1
        doc_update = doc_generator(self.key, num_item_start_for_crud,
                                   self.num_items,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   target_vbucket=self.target_vbucket,
                                   vbuckets=self.vbuckets)

        expected_num_items = self.num_items
        num_of_mutations = 1

        if doc_op == "update":
            task = self.task.async_load_gen_docs(self.cluster, def_bucket,
                                                 doc_update, "update", 0,
                                                 batch_size=10,
                                                 process_concurrency=8,
                                                 replicate_to=self.replicate_to,
                                                 persist_to=self.persist_to,
                                                 timeout_secs=self.sdk_timeout,
                                                 retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)
            # TODO: Proc to verify the mutation value in each doc
            # self.verify_doc_mutation(doc_update, num_of_mutations)
        elif doc_op == "delete":
            task = self.task.async_load_gen_docs(self.cluster, def_bucket,
                                                 doc_update, "delete", 0,
                                                 batch_size=10,
                                                 process_concurrency=8,
                                                 replicate_to=self.replicate_to,
                                                 persist_to=self.persist_to,
                                                 timeout_secs=self.sdk_timeout,
                                                 retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)
            expected_num_items = self.num_items - (self.num_items-num_item_start_for_crud)
        else:
            self.log.warning("Unsupported doc_operation")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(expected_num_items)

    def test_large_doc_size(self):
        # bucket size=256MB, when Bucket gets filled 236MB then test starts failing
        # document size=2MB, No of docs = 221 , load 250 docs
        # generate docs with size >= 1MB , See MB-29333

        self.doc_size *= 1024000
        gens_load = self.generate_docs_bigdata(docs_per_day=self.num_items,
                                               document_size=self.doc_size)
        self.print_cluster_stat_task = self.cluster_util.async_print_cluster_stats()
        for bucket in self.bucket_util.buckets:
            print_ops_task = self.bucket_util.async_print_bucket_ops(bucket)
            task = self.task.async_load_gen_docs(self.cluster, bucket,
                                                 gens_load, "create", 0,
                                                 batch_size=10,
                                                 process_concurrency=8,
                                                 replicate_to=self.replicate_to,
                                                 persist_to=self.persist_to,
                                                 timeout_secs=self.sdk_timeout,
                                                 retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)
            print_ops_task.end_task()
            self.task_manager.get_task_result(print_ops_task)

        # check if all the documents(250) are loaded with default timeout
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_large_doc_20MB(self):
        # test reproducer for MB-29258,
        # Load a doc which is greater than 20MB
        # with compression enabled and check if it fails
        # check with compression_mode as active, passive and off
        gens_load = self.generate_docs_bigdata(docs_per_day=1,
                                               document_size=(self.doc_size * 1024000))
        self.print_cluster_stat_task = self.cluster_util.async_print_cluster_stats()
        for bucket in self.bucket_util.buckets:
            print_ops_task = self.bucket_util.async_print_bucket_ops(bucket)
            task = self.task.async_load_gen_docs(self.cluster, bucket,
                                                 gens_load, "create", 0,
                                                 batch_size=10,
                                                 process_concurrency=8,
                                                 replicate_to=self.replicate_to,
                                                 persist_to=self.persist_to,
                                                 timeout_secs=self.sdk_timeout,
                                                 retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)
            print_ops_task.end_task()
            self.task_manager.get_task_result(print_ops_task)

        for bucket in self.bucket_util.buckets:
            if self.doc_size > 20:
                # failed with error "Data Too Big" when document size > 20MB
                self.bucket_util.verify_stats_all_buckets(0)
            else:
                self.bucket_util.verify_stats_all_buckets(1)
                gens_update = self.generate_docs_bigdata(docs_per_day=1,
                                                         document_size=(21 * 1024000))
                print_ops_task = self.bucket_util.async_print_bucket_ops(bucket)
                task = self.task.async_load_gen_docs(self.cluster, bucket,
                                                     gens_update, "create", 0,
                                                     batch_size=10,
                                                     process_concurrency=8,
                                                     replicate_to=self.replicate_to,
                                                     persist_to=self.persist_to,
                                                     timeout_secs=self.sdk_timeout,
                                                     retries=self.sdk_retries)
                self.task.jython_task_manager.get_task_result(task)
                print_ops_task.end_task()
                self.task_manager.get_task_result(print_ops_task)
                self.bucket_util.verify_stats_all_buckets(1)

    def test_diag_eval_curl(self):
        # Check if diag/eval can be done only by local host
        # epengine.basic_ops.basic_ops.test_diag_eval_curl,disable_diag_eval_non_local=True

        self.disable_diag_eval_on_non_local_host = self.input.param("disable_diag_eval_non_local", False)
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
        if self.disable_diag_eval_on_non_local_host:
            command = cmd_base + '-X POST -d \'ns_config:set(allow_nonlocal_eval, false).\''
            output, error = shell.execute_command(command)

        # check ip address on diag/eval will not work fine when allow_nonlocal_eval is disabled
        cmd = []
        cmd_base = 'curl http://{0}:{1}@{2}:{3}/diag/eval '.format(self.cluster.master.rest_username,
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

    def verify_stat(self, items, value="active"):
        mc = MemcachedClient(self.cluster.master.ip, 11210)
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
        '''
        test reproducer for MB-29272,
        Load some documents with compression mode set to active
        get the cbstats
        change compression mode to off and wait for minimum 250ms
        Load some more documents and check the compression is not done
        epengine.basic_ops.basic_ops.test_compression_active_and_off,items=10000,compression_mode=active

        :return:
        '''
        # Load some documents with compression mode as active
        gen_create = BlobGenerator('eviction', 'eviction-', self.value_size,
                                   end=self.num_items)
        gen_create2 = BlobGenerator('eviction2', 'eviction2-', self.value_size,
                                    end=self.num_items)
        def_bucket = self.bucket_util.get_all_buckets()[0]
        print_ops_task = self.bucket_util.async_print_bucket_ops(def_bucket)
        task = self.task.async_load_gen_docs(self.cluster, def_bucket,
                                             gen_create, "create", 0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        print_ops_task.end_task()

        remote = RemoteMachineShellConnection(self.cluster.master)
        for bucket in self.buckets:
            # change compression mode to off
            output, _ = remote.execute_couchbase_cli(cli_command='bucket-edit',
                                                     cluster_host="localhost:8091",
                                                     user=self.cluster.master.rest_username,
                                                     password=self.cluster.master.rest_password,
                                                     options='--bucket=%s --compression-mode off' % bucket.name)
            self.assertTrue(' '.join(output).find('SUCCESS') != -1,
                            'compression mode set to off')

            # sleep for 10 sec (minimum 250sec)
            time.sleep(10)

        # Load data and check stats to see compression
        # is not done for newly added data
        print_ops_task = self.bucket_util.async_print_bucket_ops(def_bucket)
        task = self.task.async_load_gen_docs(self.cluster, def_bucket,
                                             gen_create2, "create", 0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        print_ops_task.end_task()

    def do_get_random_key(self):
        # MB-31548, get_Random key gets hung sometimes.
        mc = MemcachedClient(self.cluster.master.ip, 11210)
        mc.sasl_auth_plain(self.cluster.master.rest_username,
                           self.cluster.master.rest_password)
        mc.bucket_select('default')

        count = 0
        while (count < 1000000):
            count += 1
            try:
                mc.get_random_key()
            except MemcachedError as error:
                self.fail("<MemcachedError #%d ``%s''>"
                          % (error.status, error.message))
            if count % 1000 == 0:
                self.log.info('The number of iteration is {}'.format(count))

