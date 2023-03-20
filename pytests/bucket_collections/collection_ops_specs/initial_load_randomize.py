from collections_helper.collections_spec_constants import MetaCrudParams

spec = {
    # Scope/Collection ops params
    MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
    MetaCrudParams.COLLECTIONS_TO_DROP: 0,

    MetaCrudParams.SCOPES_TO_DROP: 0,
    MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 0,
    MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 0,

    MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 0,

    # Only dropped scope/collection will be created.
    # While scope recreated all prev collection will also be created
    # In both the collection creation case, previous maxTTL value of
    # individual collection is considered
    MetaCrudParams.SCOPES_TO_RECREATE: 0,
    MetaCrudParams.COLLECTIONS_TO_RECREATE: 0,

    # Applies only for the above listed scope/collection operations
    MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
    MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
    MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

    # Doc loading params
    "doc_crud": {
        MetaCrudParams.DocCrud.RANDOMIZE_VALUE: False,

        # This applies to all collections created during following steps:
        # COLLECTIONS_TO_ADD_FOR_NEW_SCOPES,
        # COLLECTIONS_TO_ADD_PER_BUCKET,
        # SCOPES_TO_RECREATE,
        # COLLECTIONS_TO_RECREATE
        MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 0,

        # Applies to all active collections selected as per the
        # COLLECTIONS_CONSIDERED_FOR_CRUD value
        MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
        MetaCrudParams.DocCrud.DOC_SIZE :1024,
        MetaCrudParams.DocCrud.RANDOMIZE_DOC_SIZE: True,

        MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 100,
        MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 0,
        MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 0,
        MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION: 0,
        MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 0,
        MetaCrudParams.DocCrud.TOUCH_PERCENTAGE_PER_COLLECTION: 0,
        # Continuous doc_ops param, Value format: (percent, num_itr)
        MetaCrudParams.DocCrud.CONT_UPDATE_PERCENT_PER_COLLECTION: (0, 0),
        MetaCrudParams.DocCrud.CONT_REPLACE_PERCENT_PER_COLLECTION: (0, 0),

        # Doc loading options supported (None as of now)
    },

    "subdoc_crud": {
        MetaCrudParams.SubDocCrud.XATTR_TEST: False,

        # Applies to all active collections selected as per the
        # COLLECTIONS_CONSIDERED_FOR_CRUD value
        MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION: 0,
        MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION: 0,
        MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION: 0,
        MetaCrudParams.SubDocCrud.LOOKUP_PER_COLLECTION: 0,

        # Sub-doc loading option supported (None as of now)
    },

    # Doc_loading task options
    MetaCrudParams.DOC_TTL: 0,
    MetaCrudParams.DURABILITY_LEVEL: "",
    MetaCrudParams.SDK_TIMEOUT: 120,  # Default is 60
    MetaCrudParams.SDK_TIMEOUT_UNIT: "seconds",
    MetaCrudParams.TARGET_VBUCKETS: "all",
    MetaCrudParams.SKIP_READ_ON_ERROR: True,    # Default is False
    MetaCrudParams.SUPPRESS_ERROR_TABLE: True,  # Default is False
    # The below is to skip populating success dictionary for reads
    MetaCrudParams.SKIP_READ_SUCCESS_RESULTS: True,  # Default is False

    MetaCrudParams.RETRY_EXCEPTIONS: [],
    MetaCrudParams.IGNORE_EXCEPTIONS: [],

    # Applies only for DocCrud / SubDocCrud operation
    MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: "all",
    MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: "all",
    MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all",

    # Number of threadpool executor workers for scope/collection drops/creates
    # for making parallel rest calls during subsequent data load
    MetaCrudParams.THREADPOOL_MAX_WORKERS: 10
}
