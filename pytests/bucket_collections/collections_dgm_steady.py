from BucketLib.BucketOperations import BucketHelper
from Cb_constants import CbServer
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_utils.cb_tools.cbstats import Cbstats
from platform_utils.remote.remote_util import RemoteMachineShellConnection


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


        doc_loading_spec["doc_crud"][MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 0
        doc_loading_spec["doc_crud"][MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 0
        doc_loading_spec["doc_crud"][MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 0

        return doc_loading_spec

    def spec_for_create_scopes_collections(self):
        doc_loading_spec = self.get_common_spec()
        doc_loading_spec[MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = 10
        doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 2
        doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 5
        doc_loading_spec[MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 10

        doc_loading_spec["doc_crud"][MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 0
        doc_loading_spec["doc_crud"][MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 0
        doc_loading_spec["doc_crud"][MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 0

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

    def custom_load(self, maxttl=0):
        self.key = "test_collections"
        start = self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items
        load_gen = doc_generator(self.key, start, start + 1)
        tasks = []
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, load_gen, "create", maxttl,
            batch_size=1000, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            active_resident_threshold=self.dgm,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            if task.fail:
                self.fail("preload dgm failed")
        self.bucket_util.print_bucket_stats()

    def data_validation(self, doc_loading_task):
        if doc_loading_task.result is False:
            self.fail("doc_loading failed while in DGM")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def test_crud_docs(self):
        self.custom_load()
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
        self.custom_load()
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
        self.custom_load()
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
        self.custom_load()
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
