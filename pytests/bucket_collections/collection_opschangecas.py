from threading import Thread

from Cb_constants import DocLoading, CbServer
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from cb_tools.cbstats import Cbstats


class OpsChangeCasTests(CollectionBase):
    def setUp(self):
        super(OpsChangeCasTests, self).setUp()
        self.bucket = self.bucket_util.buckets[0]
        # To override default num_items to '0'
        self.num_items = self.input.param("num_items", 10)
        self.key = "test_collections"
        self.doc_size = self.input.param("doc_size", 256)
        self.doc_ops = self.input.param("doc_ops", None)
        self.mutate_times = self.input.param("mutate_times", 10)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

    def tearDown(self):
        super(OpsChangeCasTests, self).tearDown()

    def verify_cas(self, ops, generator, bucket, scope, collection):
        """
        Verify CAS value manipulation.

        For update we use the latest CAS value return by set()
        to do the mutation again to see if there is any exceptions.
        We should be able to mutate that item with the latest CAS value.
        For delete(), after it is called, we try to mutate that item with the
        cas value returned by delete(). We should see SDK Error.
        Otherwise the test should fail.
        For expire, We want to verify using the latest CAS value of that item
        can not mutate it because it is expired already.
        """

        client = self.sdk_client_pool.get_client_for_bucket(
            bucket, scope, collection)
        while generator.has_next():
            key, value = generator.next()
            vb_of_key = self.bucket_util.get_vbucket_num_for_key(key)
            active_node_ip = None
            for node_ip in self.shell_conn.keys():
                if vb_of_key in self.vb_details[node_ip]["active"]:
                    active_node_ip = node_ip
                    break
            self.log.info("Performing %s on %s:%s:%s key %s"
                          % (ops, bucket.name, scope, collection, key))
            if ops in [DocLoading.Bucket.DocOps.UPDATE,
                       DocLoading.Bucket.DocOps.TOUCH]:
                for x in range(self.mutate_times):
                    old_cas = client.crud(DocLoading.Bucket.DocOps.READ, key,
                                          timeout=10)["cas"]
                    if ops == DocLoading.Bucket.DocOps.UPDATE:
                        result = client.crud(
                            DocLoading.Bucket.DocOps.REPLACE, key, value,
                            durability=self.durability_level,
                            cas=old_cas)
                    else:
                        prev_exp = 0
                        for exp in [0, 60, 0, 0]:
                            result = client.crud(
                                DocLoading.Bucket.DocOps.TOUCH, key,
                                exp=exp,
                                durability=self.durability_level,
                                timeout=self.sdk_timeout)
                            if exp == prev_exp:
                                if result["cas"] != old_cas:
                                    self.log_failure("CAS updated for "
                                                     "touch with same exp: %s"
                                                     % result)
                            else:
                                if result["cas"] == old_cas:
                                    self.log_failure(
                                        "CAS not updated %s == %s"
                                        % (old_cas, result["cas"]))
                                old_cas = result["cas"]
                            prev_exp = exp

                    if result["status"] is False:
                        self.sdk_client_pool.release_client(client)
                        self.log_failure("Touch / replace with cas failed")
                        return

                    new_cas = result["cas"]
                    if ops == DocLoading.Bucket.DocOps.UPDATE:
                        if old_cas == new_cas:
                            self.log_failure("CAS old (%s) == new (%s)"
                                             % (old_cas, new_cas))

                        if result["value"] != value:
                            self.log_failure("Value mismatch. %s != %s"
                                             % (result["value"], value))
                        else:
                            self.log.debug(
                                "Mutate %s with CAS %s successfully! "
                                "Current CAS: %s"
                                % (key, old_cas, new_cas))

                    active_read = client.crud(DocLoading.Bucket.DocOps.READ,
                                              key, timeout=self.sdk_timeout)
                    active_cas = active_read["cas"]
                    replica_cas = -1
                    cas_in_active_node = \
                        self.cb_stat[active_node_ip].vbucket_details(
                            bucket.name)[str(vb_of_key)]["max_cas"]
                    if str(cas_in_active_node) != str(new_cas):
                        self.log_failure("CbStats CAS mismatch. %s != %s"
                                         % (cas_in_active_node, new_cas))

                    poll_count = 0
                    max_retry = 5
                    while poll_count < max_retry:
                        replica_read = client.get_from_all_replicas(key)[0]
                        replica_cas = replica_read["cas"]
                        if active_cas == replica_cas \
                                or self.durability_level:
                            break
                        poll_count = poll_count + 1
                        self.sleep(1, "Retry read CAS from replica..")

                    if active_cas != replica_cas:
                        self.log_failure("Replica cas mismatch. %s != %s"
                                         % (new_cas, replica_cas))
            elif ops == DocLoading.Bucket.DocOps.DELETE:
                old_cas = client.crud(DocLoading.Bucket.DocOps.READ, key,
                                      timeout=10)["cas"]
                result = client.crud(DocLoading.Bucket.DocOps.DELETE, key,
                                     durability=self.durability_level,
                                     timeout=self.sdk_timeout)
                self.log.debug("CAS after delete of key %s: %s"
                               % (key, result["cas"]))
                result = client.crud(DocLoading.Bucket.DocOps.REPLACE,
                                     key, "test",
                                     durability=self.durability_level,
                                     timeout=self.sdk_timeout,
                                     cas=old_cas)
                if result["status"] is True:
                    self.log_failure("The item should already be deleted")
                if SDKException.DocumentNotFoundException \
                        not in result["error"]:
                    self.log_failure("Invalid Exception: %s" % result)
                if result["cas"] != 0:
                    self.log_failure("Delete returned invalid cas: %s, "
                                     "Expected 0" % result["cas"])
                if result["cas"] == old_cas:
                    self.log_failure("Deleted doc returned old cas: %s "
                                     % old_cas)
            elif ops == "expire":
                maxttl = 5
                old_cas = client.crud(DocLoading.Bucket.DocOps.READ, key,
                                      timeout=10)["cas"]
                result = client.crud(DocLoading.Bucket.DocOps.TOUCH, key,
                                     exp=maxttl)
                if result["status"] is True:
                    if result["cas"] == old_cas:
                        self.log_failure("Touch failed to update CAS")
                else:
                    self.log_failure("Touch operation failed")

                self.sleep(maxttl+1, "Wait for item to expire")
                result = client.crud(DocLoading.Bucket.DocOps.REPLACE,
                                     key, "test",
                                     durability=self.durability_level,
                                     timeout=self.sdk_timeout,
                                     cas=old_cas)
                if result["status"] is True:
                    self.log_failure("Able to mutate %s with old cas: %s"
                                     % (key, old_cas))
                if SDKException.DocumentNotFoundException \
                        not in result["error"]:
                    self.log_failure("Invalid error after expiry: %s" % result)
        self.sdk_client_pool.release_client(client)

    def ops_change_cas(self):
        """
        CAS value manipulation by update, delete, expire test.
        We load a certain number of items. Then for half of them, we use
        MemcachedClient cas() method to mutate those item values in order
        to change CAS value of those items.
        We use MemcachedClient set() to set a quarter of the items expired.
        We also use MemcachedClient delete() to delete a quarter of the items
        """
        gen_update = doc_generator(self.key, 0, self.num_items/2,
                                   doc_size=self.doc_size)
        gen_delete = doc_generator(self.key,
                                   self.num_items/2,
                                   (self.num_items * 3 / 4),
                                   doc_size=self.doc_size)
        gen_expire = doc_generator(self.key,
                                   (self.num_items * 3 / 4),
                                   self.num_items,
                                   doc_size=self.doc_size)

        # Create cbstat objects
        self.shell_conn = dict()
        self.cb_stat = dict()
        self.vb_details = dict()
        for node in self.cluster_util.get_kv_nodes():
            self.vb_details[node.ip] = dict()
            self.vb_details[node.ip]["active"] = list()
            self.vb_details[node.ip]["replica"] = list()

            self.shell_conn[node.ip] = RemoteMachineShellConnection(node)
            self.cb_stat[node.ip] = Cbstats(self.shell_conn[node.ip])
            self.vb_details[node.ip]["active"] = \
                self.cb_stat[node.ip].vbucket_list(self.bucket.name, "active")
            self.vb_details[node.ip]["replica"] = \
                self.cb_stat[node.ip].vbucket_list(self.bucket.name, "replica")

        collections = BucketUtils.get_random_collections(
                                    self.bucket_util.buckets, 2, 2, 1)
        for bucket_name, scope_dict in collections.iteritems():
            bucket = self.bucket_util.get_bucket_obj(self.bucket_util.buckets,
                                                     bucket_name)
            for scope_name, collection_dict in scope_dict["scopes"].items():
                for c_name, c_data in collection_dict["collections"].items():
                    threads = list()
                    if self.doc_ops is not None:
                        if DocLoading.Bucket.DocOps.UPDATE in self.doc_ops:
                            thread = Thread(
                                target=self.verify_cas,
                                args=[DocLoading.Bucket.DocOps.UPDATE,
                                      gen_update, bucket, scope_name, c_name])
                            thread.start()
                            threads.append(thread)
                        if DocLoading.Bucket.DocOps.TOUCH in self.doc_ops:
                            thread = Thread(
                                target=self.verify_cas,
                                args=[DocLoading.Bucket.DocOps.TOUCH,
                                      gen_update, bucket, scope_name, c_name])
                            thread.start()
                            threads.append(thread)
                        if DocLoading.Bucket.DocOps.DELETE in self.doc_ops:
                            thread = Thread(
                                target=self.verify_cas,
                                args=[DocLoading.Bucket.DocOps.DELETE,
                                      gen_delete, bucket, scope_name, c_name])
                            thread.start()
                            threads.append(thread)
                        if "expire" in self.doc_ops:
                            thread = Thread(target=self.verify_cas,
                                            args=["expire", gen_expire,
                                                  bucket, scope_name, c_name])
                            thread.start()
                            threads.append(thread)

                    # Wait for all threads to complete
                    for thread in threads:
                        thread.join()

        # Validate test failure
        self.validate_test_failure()

    def touch_test(self):
        self.log.info("Loading bucket %s into %s%% DGM"
                      % (self.bucket.name, self.active_resident_threshold))
        load_gen = doc_generator(self.key, 0, self.num_items,
                                 doc_size=self.doc_size)
        dgm_gen = doc_generator(self.key, self.num_items, self.num_items+1)
        dgm_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, dgm_gen,
            DocLoading.Bucket.DocOps.CREATE, 0,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=10,
            process_concurrency=4,
            active_resident_threshold=self.active_resident_threshold,
            sdk_client_pool=self.sdk_client_pool)
        self.task_manager.get_task_result(dgm_task)

        self.log.info("Touch initial self.num_items docs which are "
                      "residing on disk due to DGM")
        client = self.sdk_client_pool.get_client_for_bucket(self.bucket)
        collections = BucketUtils.get_random_collections([self.bucket],
                                                         2, 2, 1)
        for bucket_name, scope_dict in collections.iteritems():
            for scope_name, collection_dict in scope_dict["scopes"].items():
                for c_name, c_data in collection_dict["collections"].items():
                    self.log.info("CAS test on collection %s: %s"
                                  % (scope_name, c_name))
                    client.select_collection(scope_name, c_name)
                    while load_gen.has_next():
                        key, _ = load_gen.next()
                        result = client.crud(DocLoading.Bucket.DocOps.TOUCH,
                                             key,
                                             durability=self.durability_level,
                                             timeout=self.sdk_timeout)
                        if result["status"] is not True:
                            self.log_failure("Touch on %s failed: %s"
                                             % (key, result))
        # change back client's scope and coll name to _default
        # since it was changed in the while loop to select different collection
        client.scope_name = CbServer.default_scope
        client.collection_name = CbServer.default_collection
        self.sdk_client_pool.release_client(client)
        self.validate_test_failure()

    def test_key_not_exists(self):
        def run_test(bucket, scope, collection):
            self.log.info("CAS test on %s:%s" % (scope, collection))

            client = self.sdk_client_pool.get_client_for_bucket(
                bucket, scope, collection)
            for _ in range(1500):
                result = client.crud(DocLoading.Bucket.DocOps.CREATE, key, val,
                                     durability=self.durability_level,
                                     timeout=self.sdk_timeout)
                if result["status"] is False:
                    self.log_failure("Create failed: %s" % result)
                create_cas = result["cas"]

                # Delete and verify get fails
                result = client.crud(DocLoading.Bucket.DocOps.DELETE, key,
                                     durability=self.durability_level,
                                     timeout=self.sdk_timeout)
                if result["status"] is False:
                    self.log_failure("Delete failed: %s" % result)
                elif result["cas"] <= create_cas:
                    self.log_failure("Invalid cas on delete: %s" % result)

                result = client.crud(DocLoading.Bucket.DocOps.READ, key,
                                     timeout=self.sdk_timeout)
                if result["status"] is True:
                    self.log_failure("Read okay after delete: %s" % result)
                elif SDKException.DocumentNotFoundException \
                        not in str(result["error"]):
                    self.log_failure("Invalid exception during read "
                                     "for non-exists key: %s" % result)

                # cas errors do not sleep the test for 10 seconds,
                # plus we need to check that the correct error is being thrown
                result = client.crud(DocLoading.Bucket.DocOps.REPLACE,
                                     key, val, exp=60,
                                     timeout=self.sdk_timeout,
                                     cas=create_cas)
                if result["status"] is True:
                    self.log_failure("Replace okay after delete: %s" % result)
                if SDKException.DocumentNotFoundException \
                        not in str(result["error"]):
                    self.log_failure("Invalid exception during read "
                                     "for non-exists key: %s" % result)
            self.sdk_client_pool.release_client(client)

        self.key = "test_key_not_exists"
        load_gen = doc_generator(self.key, 0, 1, doc_size=256)
        key, val = load_gen.next()

        collections = BucketUtils.get_random_collections([self.bucket],
                                                         2, 2, 1)
        threads = list()
        for bucket_name, scope_dict in collections.iteritems():
            bucket_obj = self.bucket_util.get_bucket_obj(
                self.bucket_util.buckets, bucket_name)
            for scope_name, collection_dict in scope_dict["scopes"].items():
                for c_name, c_data in collection_dict["collections"].items():
                    thread = Thread(target=run_test,
                                    args=[bucket_obj, scope_name, c_name])
                    thread.start()
                    threads.append(thread)
        for thread in threads:
            thread.join()

        self.validate_test_failure()
        # Validate doc count as per bucket collections
        self.bucket_util.validate_docs_per_collections_all_buckets()
