from Cb_constants import CbServer
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from sdk_exceptions import SDKException


class PlasmaBaseTest(BaseTestCase):
    def setUp(self):
        super(PlasmaBaseTest, self).setUp()
        self.retry_exceptions = list([SDKException.AmbiguousTimeoutException,
                                      SDKException.DurabilityImpossibleException,
                                      SDKException.DurabilityAmbiguousException])
        self.nodes_init = self.input.param("nodes_init", 1)
        self.num_items = self.input.param("num_items", 100)
        self.skip_cleanup = self.input.param("skip_cleanup", False)
        self.fragmentation = int(self.input.param("fragmentation", 50))

        self.log.info("Nodes init value is:" + str(self.nodes_init))
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.services = list()
        self.bucket_name = self.input.param("bucket_name",
                                            None)
        self.magma_buckets = self.input.param("magma_buckets", 0)
        # Get the services needs to store for the nodes
        if self.services_init:
            services = list()
            for service in self.services_init.split("-"):
                self.services.append(service.replace(":", ","))
            services = self.services[1:] if len(self.services) > 1 else None

        # Perform Add nodes + rebalance operation
        self.task.rebalance([self.cluster.master], nodes_init, [], services=services)

        # Add bucket storage
        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.standard_buckets)
        if self.standard_buckets == 1:
            self.bucket_util.create_default_bucket(
                bucket_type=self.bucket_type,
                replica=self.num_replicas,
                storage=self.bucket_storage,
                eviction_policy=self.bucket_eviction_policy)
        else:
            buckets_created = self.bucket_util.create_multiple_buckets(
                self.cluster.master,
                self.num_replicas,
                bucket_count=self.standard_buckets,
                bucket_type=self.bucket_type,
                storage={"couchstore": self.standard_buckets - self.magma_buckets,
                         "magma": self.magma_buckets},
                eviction_policy=self.bucket_eviction_policy,
                bucket_name=self.bucket_name,
                fragmentation_percentage=self.fragmentation)
            self.assertTrue(buckets_created, "Unable to create multiple buckets")

        self.buckets = self.cluster.buckets
        self.num_collections = self.input.param("num_collections", 1)
        self.num_scopes = self.input.param("num_scopes", 1)

        # Creation of scopes of num_scopes is > 1
        scope_prefix = "Scope"
        for bucket in self.cluster.buckets:
            for i in range(1, self.num_scopes):
                scope_name = scope_prefix + str(i)
                self.log.info("Creating bucket::scope {} {}\
                        ".format(bucket.name, scope_name))
                self.bucket_util.create_scope(self.cluster.master,
                                              bucket,
                                              {"name": scope_name})
                self.sleep(2)
        self.scopes = self.buckets[0].scopes.keys()
        self.log.info("Scopes list is {}".format(self.scopes))

        collection_prefix = "FunctionCollection"
        # Creation of collection of num_collections is > 1
        for bucket in self.cluster.buckets:
            for scope_name in self.scopes:
                for i in range(1, self.num_collections):
                    collection_name = collection_prefix + str(i)
                    self.log.info("Creating scope::collection {} {}\
                            ".format(scope_name, collection_name))
                    self.bucket_util.create_collection(
                        self.cluster.master, bucket,
                        scope_name, {"name": collection_name})
                    self.sleep(2)
        self.collections = self.buckets[0].scopes[self.scope_name].collections.keys()
        self.log.debug("Collections list == {}".format(self.collections))

        # load initial items in the bucket
        self.gen_create = self._doc_generator(0, self.num_items)
        self._load_all_buckets(self.cluster, self.gen_create,
                               "create", 0,
                               batch_size=self.batch_size)
        self.bucket_util._wait_for_stats_all_buckets()

    def _doc_generator(self, start, end):
        return doc_generator(self.key, start, end,
                             doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=self.cluster_util.vbuckets,
                             key_size=self.key_size,
                             randomize_doc_size=self.randomize_doc_size,
                             randomize_value=self.randomize_value,
                             mix_key_size=self.mix_key_size)

    def _load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                          only_store_hash=True, batch_size=1000, pause_secs=1,
                          timeout_secs=30, compression=True):
        retry_exceptions_local = self.retry_exceptions \
                                 + [SDKException.RequestCanceledException]
        # TODO:
        # Add support for loading element for all the scopes with bucket storage

        tasks_info = self.bucket_util.sync_load_all_buckets(
            cluster, kv_gen, op_type, exp, flag,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=timeout_secs,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs, sdk_compression=compression,
            process_concurrency=self.process_concurrency,
            retry_exceptions=retry_exceptions_local,
            scope=self.scope_name,
            collection=self.collection_name,
            sdk_client_pool=self.sdk_client_pool)
        self.assertTrue(self.bucket_util.doc_ops_tasks_status(tasks_info),
                        "Doc_ops failed in rebalance_base._load_all_buckets")
        return tasks_info
