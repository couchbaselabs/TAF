from collections_helper.collections_spec_constants import MetaCrudParams

spec = {
    # Scope/Collection ops params
    MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
    MetaCrudParams.COLLECTIONS_TO_DROP: 50,

    MetaCrudParams.SCOPES_TO_DROP: 2,
    MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 3,
    MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 2,

    MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 2,

    MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
    MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
    MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

    # Doc loading params
    "doc_crud": {

        MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 5000,

        MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
        MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 20,
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

    # Doc_loading task options
    MetaCrudParams.DOC_TTL: 0,
    MetaCrudParams.DURABILITY_LEVEL: "",
    MetaCrudParams.SDK_TIMEOUT: 120, # Default is 60
    MetaCrudParams.SDK_TIMEOUT_UNIT: "seconds",
    MetaCrudParams.TARGET_VBUCKETS: "all",
    MetaCrudParams.SKIP_READ_ON_ERROR: True,
    MetaCrudParams.SUPPRESS_ERROR_TABLE: True,
    # The below is to skip populating success dictionary for reads
    MetaCrudParams.SKIP_READ_SUCCESS_RESULTS: True, # Default is False


    MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: "all",
    MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: "all",
    MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all"
}
