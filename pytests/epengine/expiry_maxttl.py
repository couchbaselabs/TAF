from random import randint
from threading import Thread

from cb_constants import DocLoading
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from basetestcase import ClusterSetup
from BucketLib.BucketOperations import BucketHelper
from error_simulation.cb_error import CouchbaseError
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException
from constants.sdk_constants.java_client import SDKConstants
from shell_util.remote_connection import RemoteMachineShellConnection
from table_view import TableView


class ExpiryMaxTTL(ClusterSetup):
    def setUp(self):
        super(ExpiryMaxTTL, self).setUp()

        # Create default bucket
        self.create_bucket(self.cluster)

        self.key = 'test_ttl_docs'

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        self.bucket_util.get_all_buckets(self.cluster)
        self.bucket_helper_obj = BucketHelper(self.cluster.master)
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Create sdk_clients for pool
        if self.cluster.sdk_client_pool:
            self.log.info("Creating SDK client pool")
            self.cluster.sdk_client_pool.create_clients(
                self.cluster.buckets[0],
                self.cluster.nodes_in_cluster,
                req_clients=self.sdk_pool_capacity,
                compression_settings=self.sdk_compression)

        self.log.info("==========Finished ExpiryMaxTTL base setup========")

    def getTargetNodes(self):
        def select_randam_node(nodes):
            rand_node_index = randint(1, self.nodes_init-1)
            if self.cluster.nodes_in_cluster[rand_node_index] not in node_list:
                nodes.append(self.cluster.nodes_in_cluster[rand_node_index])

        node_list = list()
        num_nodes_affected = 1
        if self.num_replicas > 1:
            num_nodes_affected = 2

        if len(self.cluster.nodes_in_cluster) > 1:
            # Choose random nodes
            while len(node_list) != num_nodes_affected:
                select_randam_node(node_list)
        return node_list

    def _load_json(self, bucket, num_items, exp=0, op_type="create"):
        self.log.info("Creating doc_generator..")
        doc_create = doc_generator(
            self.key, 0, num_items, doc_size=self.doc_size,
            doc_type="json", target_vbucket=self.target_vbucket,
            vbuckets=self.cluster.vbuckets)
        self.log.info("doc_generator created")
        task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_create, op_type, exp,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            compression=self.sdk_compression,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
        return

    def load_docs_in_parallel(self, bucket,
                              non_ttl_gen, ttl_gen,
                              non_ttl_task_property, ttl_task_property):
        tasks_info = dict()
        self.log.info("Starting doc_loading tasks")
        non_ttl_task = self.task.async_load_gen_docs(
            self.cluster, bucket, non_ttl_gen,
            non_ttl_task_property["op_type"], 0,
            batch_size=10, process_concurrency=4,
            replicate_to=non_ttl_task_property["replicate_to"],
            persist_to=non_ttl_task_property["persist_to"],
            durability=non_ttl_task_property["durability"],
            timeout_secs=self.sdk_timeout,
            compression=self.sdk_compression,
            load_using=self.load_docs_using)
        ttl_task = self.task.async_load_gen_docs(
            self.cluster, bucket, ttl_gen,
            ttl_task_property["op_type"], self.maxttl,
            batch_size=10, process_concurrency=4,
            replicate_to=ttl_task_property["replicate_to"],
            persist_to=ttl_task_property["persist_to"],
            durability=ttl_task_property["durability"],
            timeout_secs=self.sdk_timeout,
            compression=self.sdk_compression, print_ops_rate=False,
            load_using=self.load_docs_using)
        tasks_info[non_ttl_task] = self.bucket_util.get_doc_op_info_dict(
            bucket, non_ttl_task_property["op_type"], 0,
            replicate_to=non_ttl_task_property["replicate_to"],
            persist_to=non_ttl_task_property["persist_to"],
            durability=non_ttl_task_property["durability"],
            timeout=self.sdk_timeout, time_unit="seconds")
        tasks_info[ttl_task] = self.bucket_util.get_doc_op_info_dict(
            bucket, non_ttl_task_property["op_type"], 0,
            replicate_to=non_ttl_task_property["replicate_to"],
            persist_to=non_ttl_task_property["persist_to"],
            durability=non_ttl_task_property["durability"],
            timeout=self.sdk_timeout, time_unit="seconds")

        # Wait for tasks completion and validate failures
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster, load_using=self.load_docs_using)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)

    def test_maxttl_lesser_doc_expiry(self):
        """
         A simple test to create a bucket with maxTTL and
         check whether new creates with greater exp are
         deleted when maxTTL has lapsed
        :return:
        """
        for bucket in self.cluster.buckets:
            self._load_json(bucket, self.num_items, exp=self.maxttl)
        self.sleep(self.maxttl, "Waiting for docs to expire as per maxTTL")
        self.bucket_util._expiry_pager(self.cluster)
        self.log.info("Calling compaction after expiry pager call")
        compact_tasks = []
        for bucket in self.cluster.buckets:
            compact_tasks.append(self.task.async_compact_bucket(
                self.cluster.master, bucket))
        for task in compact_tasks:
            self.task.jython_task_manager.get_task_result(task)
            self.assertTrue(task.result, "Compaction failed due to:"
                                         + str(task.exception))
        self.sleep(20, "Waiting for item count to come down...")

        for bucket in self.cluster.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry {0}s, maxTTL {1}s, "
                          "after {2}s, item count {3}"
                          .format(self.maxttl, self.maxttl,
                                  self.maxttl, items))
            if items > 0:
                self.fail("Bucket maxTTL of {0} is not honored"
                          .format(self.maxttl))
            else:
                self.log.info("SUCCESS: Doc expiry={0}s, maxTTL {1}s, "
                              "after {2}s, item count {3}"
                              .format(int(self.maxttl), self.maxttl,
                                      self.maxttl, items))

    def test_maxttl_greater_doc_expiry(self):
        """
        maxTTL is set to 200s in this test,
        Docs have lesser TTL.
        :return:
        """
        for bucket in self.cluster.buckets:
            self._load_json(bucket, self.num_items, exp=int(self.maxttl)-100)
        self.sleep(self.maxttl-100, "Waiting for docs to expire as per maxTTL")
        self.bucket_util._expiry_pager(self.cluster)
        self.sleep(20, "Waiting for item count to come down...")
        for bucket in self.cluster.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry={0}s, maxTTL={1}s, "
                          "after {2}s, item count={3}"
                          .format(int(self.maxttl) - 100, self.maxttl-100,
                                  self.maxttl-100, items))
            if items == 0:
                self.log.info("SUCCESS: Docs with lesser expiry deleted")
            else:
                self.fail("FAIL: Doc with lesser expiry still exists past ttl")

    def test_set_maxttl_on_existing_bucket(self):
        """
        1. Create a bucket with no max_ttl
        2. Upload 1000 docs with 'doc_init_expiry' seconds
        3. Set maxTTL on bucket as 'doc_new_expiry' seconds
        4. After 'doc_init_expiry' seconds, run expiry pager and
           get item count, must be 1000
        5. After 'doc_new_expiry' seconds, run expiry pager again and
           get item count, must be 0
        6. Now load another set of docs with 'doc_init_expiry' seconds
        7. Run expiry pager after 'doc_new_expiry' seconds and
           get item count, must be 0
        """
        doc_ttl = 180
        bucket_ttl = 60
        def_bucket = self.cluster.buckets[0]

        self.log.info("Inserting docs with expiry=%s" % doc_ttl)
        self._load_json(def_bucket, self.num_items, exp=doc_ttl)

        self.log.info("Updating bucket ttl=%s" % bucket_ttl)
        self.bucket_util.update_all_bucket_maxTTL(self.cluster,
                                                  maxttl=bucket_ttl)

        self.sleep(bucket_ttl, "Wait for bucket_ttl expiry time")
        self.bucket_util._expiry_pager(self.cluster)
        self.sleep(20, "Waiting for items to get purged")

        items = self.bucket_helper_obj.get_active_key_count(def_bucket.name)
        self.assertTrue(items == self.num_items,
                        "After %ss, items expected (%s) != actual (%s). "
                        "Items with larger doc_ttl expired before "
                        "maxTTL updation deleted!"
                        % (bucket_ttl, self.num_items, items))

        self.sleep(doc_ttl-bucket_ttl, "Wait for doc_ttl-bucket_ttl time")
        self.bucket_util._expiry_pager(self.cluster)
        self.sleep(20, "Waiting for items to get purged")

        items = self.bucket_helper_obj.get_active_key_count(def_bucket.name)
        self.assertTrue(items == 0,
                        "After %ss, items expected (0) != actual (%s). "
                        "Items with not greater expiry set before "
                        "maxTTL updation not deleted after elapsed TTL!"
                        % (doc_ttl-bucket_ttl, items))

        self.log.info("Inserting docs with expiry=%s" % doc_ttl)
        for def_bucket in self.cluster.buckets:
            self._load_json(def_bucket, self.num_items, exp=doc_ttl)

        self.sleep(bucket_ttl, "Wait only till bucket_ttl time")
        self.bucket_util._expiry_pager(self.cluster)
        self.sleep(20, "Waiting for items to get purged")

        items = self.bucket_helper_obj.get_active_key_count(def_bucket.name)
        self.assertTrue(items == 0,
                        "After %ss, items expected (0) != actual (%s). "
                        "Items with not greater expiry not "
                        "deleted after elapsed maxTTL!" % (bucket_ttl, items))

    def test_maxttl_possible_values(self):
        """
        Test
        1. min - 0
        2. max - 2147483647q
        3. default - 0
        4. negative values, date, string
        """
        # default
        default_bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        if default_bucket.maxTTL != 0:
            self.fail("FAIL: default maxTTL if left unset must be 0 but is {0}"
                      .format(default_bucket.maxTTL))
        self.log.info("Verified: default maxTTL if left unset is {0}"
                      .format(default_bucket.maxTTL))

        # max value
        try:
            self.bucket_util.update_all_bucket_maxTTL(self.cluster,
                                                      maxttl=2147483648)
        except Exception as e:
            self.log.info("Expected exception : {0}".format(e))
            try:
                self.bucket_util.update_all_bucket_maxTTL(self.cluster,
                                                          maxttl=2147483647)
            except Exception as e:
                self.fail("Unable to set max value for maxTTL=2147483647: {0}"
                          .format(e))
            else:
                self.log.info("Verified: Max value permitted is 2147483647")
        else:
            self.fail("Able to set maxTTL greater than 2147483647")

        # min value
        try:
            self.bucket_util.update_all_bucket_maxTTL(self.cluster,
                                                      maxttl=0)
        except Exception as e:
            self.fail("Unable to set maxTTL=0, the min permitted value: {0}"
                      .format(e))
        else:
            self.log.info("Verified: Min value permitted is 0")

        # negative value
        try:
            self.bucket_util.update_all_bucket_maxTTL(self.cluster,
                                                      maxttl=-60)
        except Exception as e:
            self.log.info("Verified: negative values denied, exception: {0}"
                          .format(e))
        else:
            self.fail("FAIL: Able to set a negative maxTTL")

        # date/string
        try:
            self.bucket_util.update_all_bucket_maxTTL(self.cluster,
                                                      maxttl="12/23/2016")
        except Exception as e:
            self.log.info("Verified: string not permitted, exception : {0}"
                          .format(e))
        else:
            self.fail("FAIL: Able to set a date string maxTTL")

    def test_update_maxttl(self):
        """
        1. Create a bucket with ttl = 200s
        2. Upload 1000 docs with exp = 100s
        3. Update ttl = 40s
        4. After 40s, run expiry pager again and get item count, must be 1000
        5. After 60s, run expiry pager again and get item count, must be 0
        6. Now load another set of docs with exp = 100s
        7. Run expiry pager after 40s and get item count, must be 0
        """
        for bucket in self.cluster.buckets:
            self._load_json(bucket, self.num_items, exp=100)
        self.bucket_util.update_all_bucket_maxTTL(self.cluster,
                                                  maxttl=40)

        self.sleep(40, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager(self.cluster)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.cluster.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry=100s, maxTTL during doc creation = 200s"
                          " updated maxttl=40s, after 40s item count = {0}"
                          .format(items))
            if items != self.num_items:
                self.fail("FAIL: Updated ttl affects docs with larger expiry "
                          "before updation!")

        self.sleep(60, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager(self.cluster)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.cluster.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry=100s, maxTTL during doc creation=200s"
                          " updated maxttl=40s, after 100s item count = {0}"
                          .format(items))
            if items != 0:
                self.fail("FAIL: Docs with 100s as expiry before "
                          "maxTTL updation still alive!")

    def test_maxttl_with_doc_updates(self):
        """
        1. Create a bucket with ttl = self.maxttl
        2. Upload 1000 docs with exp = self.maxttl - 20
        3. After 20s, Update docs with exp = self.maxttl
        4. After 40s, run expiry pager again and get item count, must be 1000
        5. After 20s, run expiry pager again and get item count, must be 0
        """
        for bucket in self.cluster.buckets:
            self._load_json(bucket, self.num_items, exp=self.maxttl-20)

        self.sleep(20, "Waiting to update docs with exp={0}s"
                       .format(self.maxttl))
        for bucket in self.cluster.buckets:
            self._load_json(bucket, self.num_items,
                            exp=self.maxttl, op_type="update")

        self.sleep(self.maxttl-20, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager(self.cluster)
        for bucket in self.cluster.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Items: {0}".format(items))
            if items != self.num_items:
                self.fail("FAIL: Docs with updated expiry deleted")

        self.sleep(20, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager(self.cluster)

        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.cluster.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Items: {0}".format(items))
            if items != 0:
                self.fail("FAIL: Docs with updated expiry not deleted after "
                          "new exp has elapsed!")

    def test_maxttl_with_sync_writes(self):
        """
        1. Load few docs without TTL
        2. Load few docs with TTL set in parallel to #1
        3. Validate docs get expiry after the TTL time
        :return:
        """

        def_bucket = self.cluster.buckets[0]
        self.maxttl = self.input.param("doc_ttl", self.maxttl)
        doc_ops_type = self.input.param("doc_ops_type", "sync;sync").split(";")

        # Create default doc_load options for TTL and non-TTL tasks
        non_ttl_task_property = dict()
        ttl_task_property = dict()

        # Create generators for TTL and non_TTL loading
        self.log.info("Creating doc_generators")
        ttl_gen_create = doc_generator(
            self.key, 0, self.num_items, doc_size=self.doc_size,
            doc_type=self.doc_type, target_vbucket=self.target_vbucket,
            vbuckets=self.cluster.vbuckets)
        non_ttl_gen_create = doc_generator(
            self.key, self.num_items, self.num_items*2, doc_size=self.doc_size,
            doc_type=self.doc_type, target_vbucket=self.target_vbucket,
            vbuckets=self.cluster.vbuckets)

        # Set durability levels based on doc_ops_type
        non_ttl_task_property["op_type"] = "create"
        ttl_task_property["op_type"] = "create"

        if doc_ops_type[0] == "sync":
            non_ttl_task_property["replicate_to"] = 0
            non_ttl_task_property["persist_to"] = 0
            non_ttl_task_property["durability"] = self.durability_level
        else:
            non_ttl_task_property["replicate_to"] = self.replicate_to
            non_ttl_task_property["persist_to"] = self.persist_to
            non_ttl_task_property["durability"] = "None"

        if doc_ops_type[1] == "sync":
            ttl_task_property["replicate_to"] = 0
            ttl_task_property["persist_to"] = 0
            ttl_task_property["durability"] = self.durability_level
        else:
            ttl_task_property["replicate_to"] = self.replicate_to
            ttl_task_property["persist_to"] = self.persist_to
            ttl_task_property["durability"] = "None"

        self.load_docs_in_parallel(def_bucket,
                                   non_ttl_gen_create, ttl_gen_create,
                                   non_ttl_task_property, ttl_task_property)
        # Validate doc_count before expiry of docs
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items*2)

        self.sleep(self.maxttl, "Sleep for maxTTL time")
        self.bucket_util._expiry_pager(self.cluster)
        self.sleep(25, "Waiting for items to be purged")

        # Read all expired docs to validate EONENT status
        ttl_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, ttl_gen_create, "read", self.maxttl,
            batch_size=10, process_concurrency=8,
            timeout_secs=self.sdk_timeout, compression=self.sdk_compression,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(ttl_task)

        # Max-TTL doc expiry validation
        self.log.info("Validating expiry of docs")
        if len(ttl_task.success.keys()) != 0:
            self.fail("Items present after MaxTTL time: %s"
                      % ttl_task.success.keys())

        invalid_exception_tbl = TableView(self.log.info)
        invalid_exception_tbl.set_headers(["Doc_Key", "CAS"])
        for doc_key, result in ttl_task.fail.items():
            if result["cas"] != 0 and result["error"] is not None:
                invalid_exception_tbl.add_row([doc_key, result["cas"]])
        invalid_exception_tbl.display("Invalid exceptions for following keys")

        if len(invalid_exception_tbl.rows) != 0:
            self.fail("Seen invalid document exception")

        # Validate doc_count after doc_expiry
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        # Document mutations after doc_expiry
        non_ttl_task_property["op_type"] = "update"
        self.load_docs_in_parallel(def_bucket,
                                   non_ttl_gen_create, ttl_gen_create,
                                   non_ttl_task_property, ttl_task_property)
        # Validate doc_count before expiry of docs
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items*2)

    def test_maxttl_with_timeout(self):
        """
        1. Stop Memcached on target_nodes based on replicas configured.
        2. Initiate doc_ops with higher sdk_timeout
        3. Sleep for time within the configured sdk_timeout
        4. Resume Memcached on target_nodes to make sure doc_ops go through
        5. Make sure maxTTL is calculated as soon as the active vbucket
           receives the mutation
        :return:
        """
        shell_conn = dict()
        target_vbuckets = list()
        target_nodes = self.getTargetNodes()
        def_bucket = self.cluster.buckets[0]
        self.maxttl = self.input.param("doc_ttl", self.maxttl)

        # Open required SDK connections before error_simulation
        gen_create = doc_generator(
            self.key, 0, self.num_items, doc_size=self.doc_size,
            doc_type=self.doc_type, target_vbucket=target_vbuckets,
            vbuckets=self.cluster.vbuckets)
        doc_op_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "create", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
            compression=self.sdk_compression, start_task=False,
            load_using=self.load_docs_using)

        # Open shell_conn and create Memcached error for testing MaxTTL
        self.log.info("1. Stopping Memcached on target_nodes")
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstats = Cbstats(node)
            target_vbuckets += cbstats.vbucket_list(def_bucket.name, "replica")
            cbstats.disconnect()
            cb_error = CouchbaseError(self.log,
                                      shell_conn[node.ip],
                                      node=node)
            cb_error.create(CouchbaseError.STOP_MEMCACHED, def_bucket.name)

        self.log.info("2. Initiating the doc_ops with doc TTL")
        self.task_manager.add_new_task(doc_op_task)

        self.sleep(self.maxttl, "3. Sleep for max_ttl time")

        # Revert Memcached error and close the shell_conn
        self.log.info("4. Resuming Memcached on target_nodes")
        for node in target_nodes:
            cb_error = CouchbaseError(self.log,
                                      shell_conn[node.ip],
                                      node=node)
            cb_error.revert(CouchbaseError.STOP_MEMCACHED, def_bucket.name)
            shell_conn[node.ip].disconnect()

        self.log.info("5. Waiting for doc_ops to complete")
        self.task.jython_task_manager.get_task_result(doc_op_task)

        self.bucket_util._expiry_pager(self.cluster, val=1)
        self.sleep(10, "6. Waiting for items to be purged")

        # Read all expired docs to validate all keys present
        doc_op_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "read",
            batch_size=10, process_concurrency=8,
            timeout_secs=self.sdk_timeout, load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(doc_op_task)

        self.log.info("7. Validating docs expired after TTL, "
                      "even before sync_write succeeds")
        if len(doc_op_task.success.keys()) == self.num_items:
            self.fail("No docs deleted after MaxTTL time: %s"
                      % doc_op_task.success.keys())

        self.sleep(10, "8. Waiting for all docs to be purged")
        # Read all expired docs to validate all keys present
        doc_op_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "read",
            batch_size=10, process_concurrency=8,
            timeout_secs=self.sdk_timeout, load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(doc_op_task)

        self.log.info("9. Validating docs expired after TTL")
        if len(doc_op_task.fail.keys()) != self.num_items:
            self.fail("Items not deleted after MaxTTL time: %s"
                      % doc_op_task.success.keys())

        # Validate cas for purged items
        keys_with_cas = list()
        for key, result in doc_op_task.fail.items():
            if result['cas'] != 0:
                keys_with_cas.append(key)
        if len(keys_with_cas) != 0:
            self.fail("Following failed keys has CAS: %s" % keys_with_cas)

        # Recreate all docs without any node issues
        doc_op_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            compression=self.sdk_compression,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(doc_op_task)

        self.log.info("10. Validating docs exists after creation")
        if len(doc_op_task.fail.keys()) != 0:
            self.fail("Doc recreate failed for keys: %s"
                      % doc_op_task.fail.keys())

        # Final doc_count validation
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

    def test_ttl_less_than_durability_timeout(self):
        """
        MB-43238
        1. Regular write with TTL 1 second for some key
        2. Disable expiry pager (to prevent raciness)
        3. Wait TTL period
        4. Disable persistence on the node with the replica vb for that key
        5. SyncWrite PersistMajority to active vBucket for that key
           (should hang)
        6. Access key on other thread to trigger expiry
        7. Observe DCP connection being torn down without fix
        """
        def perform_sync_write():
            client.crud(
                DocLoading.Bucket.DocOps.CREATE, key, {},
                durability=SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY,
                timeout=60)

        doc_ttl = 5
        target_node = None
        key = "test_ttl_doc"
        vb_for_key = self.bucket_util.get_vbucket_num_for_key(key)
        bucket = self.cluster.buckets[0]
        cb_stat_obj = dict()

        for target_node in self.cluster.nodes_in_cluster:
            cb_stat_obj[target_node.ip] = Cbstats(target_node)

        # Find target node for replica VB
        for target_node in self.cluster.nodes_in_cluster:
            if vb_for_key in cb_stat_obj[target_node.ip].vbucket_list(
                    bucket.name, "replica"):
                break

        self.log.info("Target node: %s, Key: %s" % (target_node.ip, key))
        self.log.info("Disabling expiry_pager")
        shell = RemoteMachineShellConnection(target_node)
        cb_ep_ctl = Cbepctl(shell)
        cb_ep_ctl.set(bucket.name, "flush_param", "exp_pager_stime", 0)

        # Create SDK client
        client = SDKClient(self.cluster, bucket)

        self.log.info("Non-sync write with TTL=%s" % doc_ttl)
        client.crud(DocLoading.Bucket.DocOps.CREATE, key, {}, exp=doc_ttl)

        self.sleep(doc_ttl, "Wait for document to expire")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        self.log.info("Stopping persistence on replica VB node using cbepctl")
        cb_ep_ctl.persistence(bucket.name, "stop")

        # Start doc_load with lesser ttl
        doc_create_thread = Thread(target=perform_sync_write)
        doc_create_thread.start()
        self.sleep(2, "Wait for sync_write thread to start")

        self.log.info("Read key from another thread to trigger expiry")
        failure = None
        result = client.crud(DocLoading.Bucket.DocOps.READ, key)
        if SDKException.DocumentNotFoundException not in str(result["error"]):
            failure = "Invalid exception: %s" % result["error"]

        self.log.info("Resuming persistence on target node")
        cb_ep_ctl.persistence(bucket.name, "start")

        # Wait for doc_create_thread to complete
        doc_create_thread.join()

        # Close SDK client and shell connections
        client.close()
        shell.disconnect()

        if failure:
            for target_node in self.cluster.nodes_in_cluster:
                cb_stat_obj[target_node.ip].disconnect()
            self.fail(failure)

        for node in self.cluster.nodes_in_cluster:
            stats = cb_stat_obj[target_node.ip].all_stats(bucket.name)
            self.log.info("Node: %s, ep_expired_access: %s"
                          % (node.ip, stats["ep_expired_access"]))
            self.assertEqual(int(stats["ep_expired_access"]), 0,
                             "%s: ep_expired_access != 0" % node.ip)

        for target_node in self.cluster.nodes_in_cluster:
            cb_stat_obj[target_node.ip].disconnect()
