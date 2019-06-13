from random import randint

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from basetestcase import BaseTestCase
from BucketLib.BucketOperations import BucketHelper
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection
from table_view import TableView


class ExpiryMaxTTL(BaseTestCase):
    def setUp(self):
        super(ExpiryMaxTTL, self).setUp()
        self.key = 'test_ttl_docs'.rjust(self.key_size, '0')

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type, maxTTL=self.maxttl,
            replica=self.num_replicas, compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()
        self.bucket_util.get_all_buckets()
        self.bucket_helper_obj = BucketHelper(self.cluster.master)
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
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
            vbuckets=self.vbuckets)
        self.log.info("doc_generator created")
        task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_create, op_type, exp,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            compression=self.sdk_compression)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
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
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            compression=self.sdk_compression)
        ttl_task = self.task.async_load_gen_docs(
            self.cluster, bucket, ttl_gen,
            ttl_task_property["op_type"], self.maxttl,
            batch_size=10, process_concurrency=4,
            replicate_to=ttl_task_property["replicate_to"],
            persist_to=ttl_task_property["persist_to"],
            durability=ttl_task_property["durability"],
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            compression=self.sdk_compression, print_ops_rate=False)
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
        self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                       self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)

    def test_maxttl_lesser_doc_expiry(self):
        """
         A simple test to create a bucket with maxTTL and
         check whether new creates with greater exp are
         deleted when maxTTL has lapsed
        :return:
        """
        for bucket in self.bucket_util.buckets:
            self._load_json(bucket, self.num_items, exp=self.maxttl)
        self.sleep(self.maxttl, "Waiting for docs to expire as per maxTTL")
        self.bucket_util._expiry_pager()
        self.sleep(20, "Waiting for item count to come down...")
        for bucket in self.bucket_util.buckets:
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
        for bucket in self.bucket_util.buckets:
            self._load_json(bucket, self.num_items, exp=int(self.maxttl)-100)
        self.sleep(self.maxttl-100, "Waiting for docs to expire as per maxTTL")
        self.bucket_util._expiry_pager()
        self.sleep(20, "Waiting for item count to come down...")
        for bucket in self.bucket_util.buckets:
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
        doc_init_expiry = 100
        doc_new_expiry = 60
        for bucket in self.bucket_util.buckets:
            self._load_json(bucket, self.num_items, exp=doc_init_expiry)
        self.bucket_util.update_all_bucket_maxTTL(maxttl=doc_new_expiry)

        self.sleep(doc_new_expiry, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager()
        self.sleep(20, "waiting for item count to come down...")

        for bucket in self.bucket_util.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s "
                          "(set after doc creation), after 60s, item count={0}"
                          .format(items))
            if items != self.num_items:
                self.fail("FAIL: Items with larger expiry before "
                          "maxTTL updation deleted!")

        self.sleep(40, "Waiting before running expiry pager...")
        self.bucket_util._expiry_pager()
        self.sleep(20, "Waiting for item count to come down...")
        for bucket in self.bucket_util.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s"
                          "(set after doc creation), after 100s,"
                          " item count = {0}".format(items))
            if items != 0:
                self.fail("FAIL: Items with not greater expiry set before "
                          "maxTTL updation not deleted after elapsed TTL!")

        for bucket in self.bucket_util.buckets:
            self._load_json(bucket, self.num_items, exp=doc_init_expiry)

        self.sleep(doc_new_expiry, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager()
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.bucket_util.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s, after 100s,"
                          " item count = {0}".format(items))
            if items != 0:
                self.fail("FAIL: Items with not greater expiry not "
                          "deleted after elapsed maxTTL!")

    def test_maxttl_possible_values(self):
        """
        Test
        1. min - 0
        2. max - 2147483647q
        3. default - 0
        4. negative values, date, string
        """
        # default
        default_bucket = self.bucket_util.get_all_buckets()[0]
        if default_bucket.maxTTL != 0:
            self.fail("FAIL: default maxTTL if left unset must be 0 but is {0}"
                      .format(default_bucket.maxTTL))
        self.log.info("Verified: default maxTTL if left unset is {0}"
                      .format(default_bucket.maxTTL))

        # max value
        try:
            self.bucket_util.update_all_bucket_maxTTL(maxttl=2147483648)
        except Exception as e:
            self.log.info("Expected exception : {0}".format(e))
            try:
                self.bucket_util.update_all_bucket_maxTTL(maxttl=2147483647)
            except Exception as e:
                self.fail("Unable to set max value for maxTTL=2147483647: {0}"
                          .format(e))
            else:
                self.log.info("Verified: Max value permitted is 2147483647")
        else:
            self.fail("Able to set maxTTL greater than 2147483647")

        # min value
        try:
            self.bucket_util.update_all_bucket_maxTTL(maxttl=0)
        except Exception as e:
            self.fail("Unable to set maxTTL=0, the min permitted value: {0}"
                      .format(e))
        else:
            self.log.info("Verified: Min value permitted is 0")

        # negative value
        try:
            self.bucket_util.update_all_bucket_maxTTL(maxttl=-60)
        except Exception as e:
            self.log.info("Verified: negative values denied, exception: {0}"
                          .format(e))
        else:
            self.fail("FAIL: Able to set a negative maxTTL")

        # date/string
        try:
            self.bucket_util.update_all_bucket_maxTTL(maxttl="12/23/2016")
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
        for bucket in self.bucket_util.buckets:
            self._load_json(bucket, self.num_items, exp=100)
        self.bucket_util.update_all_bucket_maxTTL(maxttl=40)

        self.sleep(40, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager()
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.bucket_util.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry=100s, maxTTL during doc creation = 200s"
                          " updated maxttl=40s, after 40s item count = {0}"
                          .format(items))
            if items != self.num_items:
                self.fail("FAIL: Updated ttl affects docs with larger expiry "
                          "before updation!")

        self.sleep(60, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager()
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.bucket_util.buckets:
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
        for bucket in self.bucket_util.buckets:
            self._load_json(bucket, self.num_items, exp=self.maxttl-20)

        self.sleep(20, "Waiting to update docs with exp={0}s"
                       .format(self.maxttl))
        for bucket in self.bucket_util.buckets:
            self._load_json(bucket, self.num_items,
                            exp=self.maxttl, op_type="update")

        self.sleep(self.maxttl-20, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager()
        for bucket in self.bucket_util.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Items: {0}".format(items))
            if items != self.num_items:
                self.fail("FAIL: Docs with updated expiry deleted")

        self.sleep(20, "waiting before running expiry pager...")
        self.bucket_util._expiry_pager()

        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.bucket_util.buckets:
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

        def_bucket = self.bucket_util.buckets[0]
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
            vbuckets=self.vbuckets)
        non_ttl_gen_create = doc_generator(
            self.key, self.num_items, self.num_items*2, doc_size=self.doc_size,
            doc_type=self.doc_type, target_vbucket=self.target_vbucket,
            vbuckets=self.vbuckets)

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
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items*2)

        self.sleep(self.maxttl, "Sleep for maxTTL time")
        self.bucket_util._expiry_pager()
        self.sleep(25, "Waiting for items to be purged")

        # Read all expired docs to validate EONENT status
        ttl_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, ttl_gen_create, "read", self.maxttl,
            batch_size=10, process_concurrency=8,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            compression=self.sdk_compression)
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
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # Document mutations after doc_expiry
        non_ttl_task_property["op_type"] = "update"
        self.load_docs_in_parallel(def_bucket,
                                   non_ttl_gen_create, ttl_gen_create,
                                   non_ttl_task_property, ttl_task_property)
        # Validate doc_count before expiry of docs
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items*2)

    def test_maxttl_with_timeout(self):
        """
        1. Stop Memcached on target_nodes based on replicas configured.
        2. Initiate doc_ops with higher sdk_timeout
        3. Sleep for time within the configured sdk_timeout
        4. Resume Memcached on target_nodes to make sure doc_ops go through
        5. Make sure maxTTL is calculated only after the mutation is succeeded
        :return:
        """
        shell_conn = dict()
        target_vbuckets = list()
        target_nodes = self.getTargetNodes()
        def_bucket = self.bucket_util.buckets[0]
        self.maxttl = self.input.param("doc_ttl", self.maxttl)

        # Open shell_conn and create Memcached error for testing MaxTTL
        self.log.info("1. Stopping Memcached on target_nodes")
        for node in target_nodes:
            shell_conn[node.ip] = RemoteMachineShellConnection(node)
            cbstats = Cbstats(shell_conn[node.ip])
            target_vbuckets += cbstats.vbucket_list(def_bucket.name, "replica")
            cb_error = CouchbaseError(self.log, shell_conn[node.ip])
            cb_error.create(CouchbaseError.STOP_MEMCACHED, def_bucket.name)

        self.log.info("2. Initiating the doc_ops with doc TTL")
        gen_create = doc_generator(
            self.key, 0, self.num_items, doc_size=self.doc_size,
            doc_type=self.doc_type, target_vbucket=target_vbuckets,
            vbuckets=self.vbuckets)
        doc_op_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "create", self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            compression=self.sdk_compression)

        self.sleep(self.sdk_timeout-30,
                   "3. Sleep for time less than sdk_timeout")

        # Revert Memcached error and close the shell_conn
        self.log.info("4. Resuming Memcached on target_nodes")
        for node in target_nodes:
            cb_error = CouchbaseError(self.log, shell_conn[node.ip])
            cb_error.revert(CouchbaseError.STOP_MEMCACHED, def_bucket.name)
            shell_conn[node.ip].disconnect()

        self.log.info("5. Waiting for doc_ops to complete")
        self.task.jython_task_manager.get_task_result(doc_op_task)

        self.bucket_util._expiry_pager(val=1)
        self.sleep(5, "6. Waiting for items to be purged (if any)")

        # Read all expired docs to validate all keys present
        doc_op_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "read", self.maxttl,
            batch_size=10, process_concurrency=8,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            compression=self.sdk_compression)
        self.task.jython_task_manager.get_task_result(doc_op_task)

        self.log.info("7. Validating docs exists before TTL")
        if len(doc_op_task.success.keys()) != self.num_items:
            self.fail("Items deleted before MaxTTL time: %s"
                      % doc_op_task.fail.keys())

        self.sleep(self.maxttl, "8. Sleep for maxTTL time")
        self.bucket_util._expiry_pager()
        self.sleep(20, "9. Waiting for items to be purged")

        # Read all expired docs to validate all keys present
        doc_op_task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "read", self.maxttl,
            batch_size=10, process_concurrency=8,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            compression=self.sdk_compression)
        self.task.jython_task_manager.get_task_result(doc_op_task)

        self.log.info("10. Validating docs purged after TTL")
        if len(doc_op_task.success.keys()) != 0:
            self.fail("Items exsits after MaxTTL time: %s"
                      % doc_op_task.success.keys())

        keys_with_cas = list()
        for key, result in doc_op_task.fail.items():
            if result['cas'] != 0:
                keys_with_cas.append(key)
        if len(keys_with_cas) != 0:
            self.fail("Following failed keys has CAS: %s" % keys_with_cas)

        # Final doc_count validation
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(0)
