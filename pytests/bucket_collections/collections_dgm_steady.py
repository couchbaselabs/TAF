from BucketLib.BucketOperations import BucketHelper
from Cb_constants import CbServer
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase
from collections_helper.collections_spec_constants import MetaCrudParams


class CollectionsDgmSteady(CollectionBase):
    def setUp(self):
        super(CollectionsDgmSteady, self).setUp()

        self.dgm = self.input.param("dgm", 40)
        self.creates = self.input.param("creates", False)

        self.bucket_util._expiry_pager()
        self.bucket = self.bucket_util.buckets[0]

    def tearDown(self):
        super(CollectionsDgmSteady, self).tearDown()

    def get_common_spec(self):
        spec = {
            # Scope/Collection ops params
            MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
            MetaCrudParams.COLLECTIONS_TO_DROP: 0,

            MetaCrudParams.SCOPES_TO_DROP: 0,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 0,
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 0,

            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 0,

            MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

            # Doc loading params
            "doc_crud": {

                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 0,

                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 20,
            },

            "subdoc_crud": {
                MetaCrudParams.SubDocCrud.XATTR_TEST: False,

                MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION: 0,
                MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION: 0,
                MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION: 0,
                MetaCrudParams.SubDocCrud.LOOKUP_PER_COLLECTION: 0,
            },

            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all"
        }
        return spec

    def spec_for_drop_scopes_collections(self):
        doc_loading_spec = self.get_common_spec()
        doc_loading_spec[MetaCrudParams.SCOPES_TO_DROP] = 10
        doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 10
        return doc_loading_spec

    def spec_for_create_scopes_collections(self):
        doc_loading_spec = self.get_common_spec()
        doc_loading_spec[MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = 10
        doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 2
        doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 5
        doc_loading_spec[MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 10
        return doc_loading_spec

    def spec_for_mix_crud_collections_and_docs(self):
        # Drop collections/scopes
        doc_loading_spec = self.get_common_spec()
        doc_loading_spec[MetaCrudParams.SCOPES_TO_DROP] = 10
        doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 10
        # Add collections/scopes
        doc_loading_spec[MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = 10
        doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 2
        doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 5
        doc_loading_spec[MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 10
        return doc_loading_spec

    def get_active_resident_threshold(self, bucket_name):
        self.rest_client = BucketHelper(self.cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket_name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        return dgm

    def custom_load(self, num_items, maxttl=0):
        start = self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items
        load_gen = doc_generator(self.key, start, start + num_items)
        tasks = []
        for _, scope in self.bucket.scopes.items():
            for _, collection in scope.collections.items():
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, self.bucket, load_gen, "create", maxttl,
                    batch_size=1000, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    compression=self.sdk_compression,
                    timeout_secs=self.sdk_timeout,
                    scope=scope.name,
                    collection=collection.name))
                self.bucket.scopes[scope.name] \
                    .collections[collection.name] \
                    .num_items += num_items
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            if task.fail:
                self.fail("preload dgm failed")
        self.bucket_util.print_bucket_stats()

    def preload(self, op="create"):
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package("dgm_load")
        if op == "delete":
            doc_loading_spec[MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
            doc_loading_spec[MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 5
        else:
            doc_loading_spec[MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 1
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size)


    def load_to_dgm(self):
        # load data until resident % goes below threshold
        threshold = self.dgm
        bucket_name = self.bucket_util.buckets[0].name
        curr_active = self.get_active_resident_threshold(bucket_name)
        while int(curr_active) >= threshold:
            self.preload()
            curr_active = self.get_active_resident_threshold(bucket_name)
            self.log.info("curr_active resident {0} %".format(curr_active))
            self.bucket_util._wait_for_stats_all_buckets()
        self.log.info("Initial dgm load done. Resident {0} %".format(curr_active))
        if (int(curr_active) < (threshold - 10)):
            self.sleep(30)
            self.preload(op="delete")
            curr_active = self.get_active_resident_threshold(bucket_name)
            self.log.info("Initial dgm load done. After sleep Resident {0} % ".format(curr_active))

    def data_validation(self, doc_loading_task):
        if doc_loading_task.result is False:
            self.fail("doc_loading failed while in DGM")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_crud_docs(self):
        self.load_to_dgm()
        doc_loading_spec = self.get_common_spec()
        if self.creates:
            doc_loading_spec[MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 20
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size)
        self.data_validation(doc_loading_task)

    def test_drop_scopes_collections(self):
        self.load_to_dgm()
        doc_loading_spec = self.spec_for_drop_scopes_collections()
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size)
        self.data_validation(doc_loading_task)

    def test_create_scopes_collections(self):
        self.load_to_dgm()
        doc_loading_spec = self.spec_for_create_scopes_collections()
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size)
        self.data_validation(doc_loading_task)

    def test_mix_crud_collections_and_docs(self):
        self.load_to_dgm()
        doc_loading_spec = self.spec_for_mix_crud_collections_and_docs()
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size)
        self.data_validation(doc_loading_task)
