from bucket_collections.collections_base import CollectionBase
from remote.remote_util import RemoteMachineShellConnection
from Cb_constants import CbServer
from error_simulation.cb_error import CouchbaseError

class BucketWarmup(CollectionBase):
    def setUp(self):
        super(BucketWarmup, self).setUp()
        self.load_spec = self.input.param("load_spec",
                                        "def_load_random_collection")
        self.bucket = self.bucket_util.buckets[0]

    def create_scope(self):
        self.bucket_util.create_scope(self.cluster.master,
                                         self.bucket,
                                         {"name": self.scope_name})

    def drop_scope(self):
        self.bucket_util.drop_scope(self.cluster.master,
                                             self.bucket,
                                             self.scope_name)
        del self.bucket.scopes[self.scope_name]

    def create_collection(self):
        self.bucket_util.create_collection(self.cluster.master,
                                                 self.bucket,
                                                 CbServer.default_scope,
                                                 {"name": self.collection_name})

    def drop_collection(self):
        self.bucket_util.drop_collection(self.cluster.master,
                                                 self.bucket,
                                                 self.scope_name,
                                                 self.collection_name)
        del self.bucket.scopes[self.scope_name] \
                           .collections[self.collection_name]

    def random_load(self):
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(self.load_spec)
        self.bucket_util.run_scenario_from_spec(self.task,
                                            self.cluster,
                                            self.bucket_util.buckets,
                                            doc_loading_spec,
                                            mutation_num=0,
                                            batch_size=self.batch_size)

    def perform_operation_during_bucket_warmup(self, during_warmup="default"):
        # stop memcached in master node
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        self.error_sim = CouchbaseError(self.log, shell_conn)
        self.error_sim.create(CouchbaseError.STOP_MEMCACHED)
        self.log.info("memcached stopped on master node")

        if during_warmup == "create_scope":
            try:
                self.scope_name = self.bucket_util.get_random_name()
                self.create_scope()
                self.log_failure("drop scope succeeded")
            except Exception as e:
                self.log.info(e)
                self.error_sim.revert(CouchbaseError.STOP_MEMCACHED)
                self.create_scope()

        elif during_warmup == "drop_scope":
            retry =5
            while retry > 0:
                scope_dict = self.bucket_util.get_random_scopes(
                                    self.bucket_util.buckets, 1, 1)
                self.scope_name = scope_dict[self.bucket.name]["scopes"].keys()[0]
                if self.scope_name != "_default":
                    break
                retry -= 1
            try:
                self.drop_scope()
                self.log_failure("drop scope succeeded")
            except Exception as e:
                self.log.info(e)
                self.error_sim.revert(CouchbaseError.STOP_MEMCACHED)
                self.drop_scope()

        elif during_warmup == "create_collection":
            self.collection_name = self.bucket_util.get_random_name()
            try:
                self.create_collection()
                self.log_failure("create collection succeeded")
            except Exception as e:
                self.log.info(e)
                self.error_sim.revert(CouchbaseError.STOP_MEMCACHED)
                self.create_collection()

        elif during_warmup == "drop_collection":
            collections = self.bucket_util.get_random_collections(
                                    self.bucket_util.buckets, 1, 1, 1)
            scope_dict = collections[self.bucket.name]["scopes"]
            self.scope_name = scope_dict.keys()[0]
            self.collection_name = scope_dict[self.scope_name]["collections"].keys()[0]
            try:
                self.drop_collection()
                self.log_failure("drop collection succeeded")
            except Exception as e:
                self.log.info(e)
                self.error_sim.revert(CouchbaseError.STOP_MEMCACHED)
                self.drop_collection()

        else:
            try:
                self.random_load()
                self.log_failure("random operation succeeded")
            except Exception as e:
                self.log.info(e)
                self.error_sim.revert(CouchbaseError.STOP_MEMCACHED)
                self.random_load()

        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_create_scope_during_warmup(self):
        self.perform_operation_during_bucket_warmup("create_scope")

    def test_drop_scope_during_warmup(self):
        self.perform_operation_during_bucket_warmup("drop_scope")

    def test_create_collection_during_warmup(self):
        self.perform_operation_during_bucket_warmup("create_collection")

    def test_delete_collection_during_warmup(self):
        self.perform_operation_during_bucket_warmup("drop_collection")

    def test_perform_random_operation_during_warmup(self):
        self.perform_operation_during_bucket_warmup()

    def tearDown(self):
        self.error_sim.revert(CouchbaseError.STOP_MEMCACHED)
