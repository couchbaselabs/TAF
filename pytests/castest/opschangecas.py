import json

from cb_tools.cbstats import Cbstats
from castest.cas_base import CasBaseTest
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException


class OpsChangeCasTests(CasBaseTest):
    def setUp(self):
        super(OpsChangeCasTests, self).setUp()
        self.key = 'test_cas_docs'.rjust(self.key_size, '0')
        self.log.info("=========Finished OpsChangeCasTests base setup=======")

    def tearDown(self):
        super(OpsChangeCasTests, self).tearDown()

    def _load_all_buckets(self, generator, op_type):
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, generator, op_type, 0,
                flag=self.item_flag,
                batch_size=10,
                process_concurrency=8,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                compression=self.sdk_compression)
            self.task.jython_task_manager.get_task_result(task)

    def verify_cas(self, ops, generator):
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

        for bucket in self.cluster.buckets:
            client = SDKClient([self.cluster.master], bucket)
            gen = generator
            while gen.has_next():
                key, value = gen.next()
                vb_of_key = self.bucket_util.get_vbucket_num_for_key(key)
                active_node_ip = None
                for node_ip in self.shell_conn.keys():
                    if vb_of_key in self.vb_details[node_ip]["active"]:
                        active_node_ip = node_ip
                        break
                self.log.info("Performing %s on key %s" % (ops, key))
                if ops in ["update", "touch"]:
                    for x in range(self.mutate_times):
                        old_cas = client.crud("read", key, timeout=10)["cas"]
                        # value = {"val": "mysql-new-value-%s" % x}
                        if ops == 'update':
                            result = client.crud(
                                "replace", key, value,
                                durability=self.durability_level,
                                cas=old_cas)
                        else:
                            prev_exp = 0
                            for exp in [0, 60, 0, 0]:
                                result = client.touch(
                                    key, exp,
                                    durability=self.durability_level,
                                    timeout=self.sdk_timeout)
                                if exp == prev_exp:
                                    if result["cas"] != old_cas:
                                        self.log_failure(
                                            "CAS updated for "
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
                            client.close()
                            self.log_failure("Touch / replace with cas failed")
                            return

                        new_cas = result["cas"]
                        if ops == 'update':
                            if old_cas == new_cas:
                                self.log_failure("CAS old (%s) == new (%s)"
                                                 % (old_cas, new_cas))

                            if result["value"] != value:
                                self.log_failure("Value mismatch. "
                                                 "%s != %s"
                                                 % (result["value"], value))
                            else:
                                self.log.debug(
                                    "Mutate %s with CAS %s successfully! "
                                    "Current CAS: %s"
                                    % (key, old_cas, new_cas))

                        active_read = client.crud("read", key,
                                                  timeout=self.sdk_timeout)
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
                elif ops == "delete":
                    old_cas = client.crud("read", key, timeout=10)["cas"]
                    result = client.crud("delete", key,
                                         durability=self.durability_level,
                                         timeout=self.sdk_timeout)
                    self.log.info("CAS after delete of key %s: %s"
                                  % (key, result["cas"]))
                    result = client.crud("replace", key, "test",
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
                    old_cas = client.crud("read", key, timeout=10)["cas"]
                    result = client.crud("touch", key, exp=self.expire_time)
                    if result["status"] is True:
                        if result["cas"] == old_cas:
                            self.log_failure("Touch failed to update CAS")
                    else:
                        self.log_failure("Touch operation failed")

                    self.sleep(self.expire_time+1, "Wait for item to expire")
                    result = client.crud("replace", key, "test",
                                         durability=self.durability_level,
                                         timeout=self.sdk_timeout,
                                         cas=old_cas)
                    if result["status"] is True:
                        self.log_failure("Able to mutate %s with old cas: %s"
                                         % (key, old_cas))
                    if SDKException.DocumentNotFoundException \
                            not in result["error"]:
                        self.log_failure("Invalid error after expiry: %s"
                                         % result)

    def ops_change_cas(self):
        """
        CAS value manipulation by update, delete, expire test.

        We load a certain number of items. Then for half of them, we use
        MemcachedClient cas() method to mutate those item values in order
        to change CAS value of those items.
        We use MemcachedClient set() to set a quarter of the items expired.
        We also use MemcachedClient delete() to delete a quarter of the items
        """

        gen_load = doc_generator('nosql', 0, self.num_items,
                                 doc_size=self.doc_size)
        gen_update = doc_generator('nosql', 0, self.num_items/2,
                                   doc_size=self.doc_size)
        gen_delete = doc_generator('nosql',
                                   self.num_items/2,
                                   (self.num_items * 3 / 4),
                                   doc_size=self.doc_size)
        gen_expire = doc_generator('nosql',
                                   (self.num_items * 3 / 4),
                                   self.num_items,
                                   doc_size=self.doc_size)
        self._load_all_buckets(gen_load, "create")
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.bucket_util._wait_for_stats_all_buckets()

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

        if self.doc_ops is not None:
            if "update" in self.doc_ops:
                self.verify_cas("update", gen_update)
            if "touch" in self.doc_ops:
                self.verify_cas("touch", gen_update)
            if "delete" in self.doc_ops:
                self.verify_cas("delete", gen_delete)
            if "expire" in self.doc_ops:
                self.verify_cas("expire", gen_expire)

        self.bucket_util._wait_for_stats_all_buckets()
        self.validate_test_failure()

    def touch_test(self):
        self.log.info("1. Loading initial set of documents")
        load_gen = doc_generator(self.key, 0, self.num_items,
                                 doc_size=self.doc_size)
        self._load_all_buckets(load_gen, "create")
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.bucket_util._wait_for_stats_all_buckets()

        self.log.info("2. Loading bucket into DGM")
        dgm_gen = doc_generator(
            self.key, self.num_items, self.num_items+1)
        dgm_task = self.task.async_load_gen_docs(
            self.cluster, self.cluster.buckets[0], dgm_gen, "create", 0,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=10,
            process_concurrency=4,
            active_resident_threshold=self.active_resident_threshold)
        self.task_manager.get_task_result(dgm_task)

        self.log.info("3. Touch intial self.num_items docs which are "
                      "residing on disk due to DGM")
        client = SDKClient([self.cluster.master],
                           self.cluster.buckets[0])
        while load_gen.has_next():
            key, _ = load_gen.next()
            result = client.crud("touch", key,
                                 durability=self.durability_level,
                                 timeout=self.sdk_timeout)
            if result["status"] is not True:
                self.log_failure("Touch on %s failed: %s" % (key, result))
        client.close()
        self.validate_test_failure()

    def _corrupt_max_cas(self, mcd, key):
        # set the CAS to -2 and then mutate to increment to -1 and
        # then it should stop there
        mcd.setWithMetaInvalid(key, json.dumps({'value': 'value2'}),
                               0, 0, 0, -2)
        # print 'max cas pt1', mcd.getMeta(key)[4]
        mcd.set(key, 0, 0, json.dumps({'value': 'value3'}))
        # print 'max cas pt2', mcd.getMeta(key)[4]
        mcd.set(key, 0, 0, json.dumps({'value': 'value4'}))
        # print 'max cas pt3', mcd.getMeta(key)[4]

    # MB-17517: Verify if max CAS somehow becomes -1 we can recover from it
    def corrupt_cas_is_healed_on_rebalance_out_in(self):

        self.log.info('Start corrupt_cas_is_healed_on_rebalance_out_in')

        KEY_NAME = 'key1'

        client = SDKClient([self.cluster.master], 'default')

        # set a key
        client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,
                                       json.dumps({'value': 'value1'}))

        # figure out which node it is on
        mc_active = client.memcached(KEY_NAME)
        mc_replica = client.memcached(KEY_NAME, replica_index=0)

        # set the CAS to -2 and then mutate to increment to -1 and
        # then it should stop there
        self._corrupt_max_cas(mc_active, KEY_NAME)

        # CAS should be 0 now, do some gets and sets to verify that
        # nothing bad happens
        resp = mc_active.get(KEY_NAME)
        self.log.info('get for {0} is {1}'.format(KEY_NAME, resp))

        # remove that node
        self.log.info('Remove the node with -1 max cas')
        rebalance = self.cluster.async_rebalance(self.servers[-1:],
                                                 [], [self.cluster.master])
        rebalance.result()
        replica_CAS = mc_replica.getMeta(KEY_NAME)[4]

        # add the node back
        self.log.info('Add the node back, the max_cas should be healed')
        rebalance = self.cluster.async_rebalance(self.servers[-1:],
                                                 [self.cluster.master], [])

        rebalance.result()

        # verify the CAS is good
        client = SDKClient([self.cluster.master], 'default')
        mc_active = client.memcached(KEY_NAME)
        active_CAS = mc_active.getMeta(KEY_NAME)[4]

        self.assertTrue(replica_CAS == active_CAS,
                        'cas mismatch active {0} replica {1}'
                        .format(active_CAS, replica_CAS))

    # One node only needed for this test
    def corrupt_cas_is_healed_on_reboot(self):
        self.log.info('Start corrupt_cas_is_healed_on_reboot')

        KEY_NAME = 'key1'

        client = SDKClient([self.cluster.master], 'default')

        # set a key
        client.memcached(KEY_NAME).set(KEY_NAME, 0, 0,
                                       json.dumps({'value': 'value1'}))

        # figure out which node it is on
        mc_active = client.memcached(KEY_NAME)

        # set the CAS to -2 and then mutate to increment to -1
        # and then it should stop there
        self._corrupt_max_cas(mc_active, KEY_NAME)

        # print 'max cas k2', mc_active.getMeta('k2')[4]

        # CAS should be 0 now, do some gets and sets to verify
        # that nothing bad happens
        # self._restart_memcache('default')
        remote = RemoteMachineShellConnection(self.cluster.master)
        remote.stop_server()
        self.sleep(30, "Wait for server to stop")
        remote.start_server()
        self.sleep(30, "Wait for server to start")

        client = SDKClient([self.cluster.master], 'default')
        mc_active = client.memcached(KEY_NAME)

        maxCas = mc_active.getMeta(KEY_NAME)[4]
        self.assertTrue(maxCas == 0,
                        'max cas after reboot is {0} != 0'.format(maxCas))

    """
    MB-21448 bug test
    Description: REPLACE_WITH_CAS on a key that has recently been deleted and
    then requested sometimes returns key exists with different CAS instead of
    key not exists error, this test only requires one node
    """
    def key_not_exists_test(self):
        client = SDKClient([self.cluster.master], self.bucket)
        load_gen = doc_generator(self.key, 0, 1,
                                 doc_size=256)
        key, val = load_gen.next()

        for _ in range(1500):
            result = client.crud("create", key, val,
                                 durability=self.durability_level,
                                 timeout=self.sdk_timeout)
            if result["status"] is False:
                self.log_failure("Create failed: %s" % result)
            create_cas = result["cas"]

            # Delete and verify get fails
            result = client.crud("delete", key,
                                 durability=self.durability_level,
                                 timeout=self.sdk_timeout)
            if result["status"] is False:
                self.log_failure("Delete failed: %s" % result)
            elif result["cas"] <= create_cas:
                self.log_failure("Delete returned invalid cas: %s" % result)

            result = client.crud("read", key,
                                 timeout=self.sdk_timeout)
            if result["status"] is True:
                self.log_failure("Read succeeded after delete: %s" % result)
            elif SDKException.DocumentNotFoundException \
                    not in str(result["error"]):
                self.log_failure("Invalid exception during read "
                                 "for non-exists key: %s" % result)

            # cas errors do not sleep the test for 10 seconds,
            # plus we need to check that the correct error is being thrown
            result = client.crud("replace", key, val, exp=60,
                                 timeout=self.sdk_timeout,
                                 cas=create_cas)
            if result["status"] is True:
                self.log_failure("Replace succeeded after delete: %s" % result)
            if SDKException.DocumentNotFoundException \
                    not in str(result["error"]):
                self.log_failure("Invalid exception during read "
                                 "for non-exists key: %s" % result)

            self.validate_test_failure()
