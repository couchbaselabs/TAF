from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection
from crash_test.constants import signum


class CrashTest(BaseTestCase):
    def setUp(self):
        super(CrashTest, self).setUp()

        self.key = 'test_docs'.rjust(self.key_size, '0')
        self.doc_ops = self.input.param("doc_ops", None)
        self.process_name = self.input.param("process", None)
        self.service_name = self.input.param("service", "data")
        self.sig_type = self.input.param("sig_type", "SIGKILL").upper()

        self.pre_warmup_stats = {}
        self.timeout = 120

        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type, ram_quota=self.bucket_size,
            replica=self.num_replicas, compression_mode="off")
        self.bucket_util.add_rbac_user()

        # Load initial documents into the buckets
        gen_create = doc_generator(self.key, 0, self.num_items)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_create, "create", 0,
                batch_size=10, process_concurrency=8)
            self.task.jython_task_manager.get_task_result(task)

        self.bucket_util.verify_unacked_bytes_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("==========Finished CrashTest setup========")

    def tearDown(self):
        super(CrashTest, self).tearDown()

    def stop_process(self):
        """
        1. Starting loading docs into the default bucket
        2. Stop the requested process, which will not impact the
           memcached operations
        3. Wait for load bucket task to complete
        4. Validate the docs for durability
        """
        new_docs = 100000
        def_bucket = self.bucket_util.buckets[0]
        gen_load = doc_generator(self.key, self.num_items,
                                 self.num_items+new_docs)
        self.num_items += new_docs
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_load, "create", 0, batch_size=10)

        remote = RemoteMachineShellConnection(self.cluster.master)
        remote.kill_process(self.process_name, self.service_name,
                            signum=signum[self.sig_type])
        self.sleep(20, "Wait before resuming the process"
                       .format(self.process_name))
        remote.kill_process(self.process_name, self.service_name,
                            signum=signum["SIGCONT"])

        # Wait for doc loading task to complete
        self.task.jython_task_manager.get_task_result(task)

        # Validate doc count
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()

    def crash_process(self):
        """
        1. Starting loading docs into the default bucket
        2. Crash the requested process, which will not impact the
           memcached operations
        3. Wait for load bucket task to complete
        4. Validate the docs for durability
        """
        new_docs = 100000
        def_bucket = self.bucket_util.buckets[0]
        gen_load = doc_generator(self.key, self.num_items,
                                 self.num_items+new_docs)
        self.num_items += new_docs
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_load, "create", 0, batch_size=10)

        remote = RemoteMachineShellConnection(self.cluster.master)
        remote.kill_process(self.process_name, self.service_name,
                            signum=signum[self.sig_type])
        self.sleep(60, "Waiting after kill the process")
        # Wait for doc loading task to complete
        self.task.jython_task_manager.get_task_result(task)

        # Validate doc count
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.bucket_util.verify_unacked_bytes_all_buckets()
