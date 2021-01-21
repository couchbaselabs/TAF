class MetaConstants(object):
    NUM_BUCKETS = "num_buckets"
    NUM_SCOPES_PER_BUCKET = "num_scopes_per_bucket"
    NUM_COLLECTIONS_PER_SCOPE = "num_collections_per_scope"
    NUM_ITEMS_PER_COLLECTION = "num_items"
    REMOVE_DEFAULT_COLLECTION = "remove_default_collection"
    CREATE_COLLECTIONS_USING_MANIFEST_IMPORT = \
        "create_collections_using_manifest_import"

    @staticmethod
    def get_params():
        param_list = list()
        for param, value in vars(MetaConstants).items():
            if not (param.startswith("_")
                    or callable(getattr(MetaConstants, param))):
                param_list.append(value)
        return param_list


class MetaCrudParams(object):
    COLLECTIONS_TO_FLUSH = "collections_to_flush"
    COLLECTIONS_TO_DROP = "collections_to_drop"
    COLLECTIONS_TO_DROP_AND_RECREATE = "collections_to_drop_and_recreate"

    SCOPES_TO_DROP = "scopes_to_drop"
    SCOPES_TO_ADD_PER_BUCKET = "scopes_to_add_per_bucket"

    COLLECTIONS_TO_ADD_FOR_NEW_SCOPES = "collections_to_add_for_new_scopes"
    COLLECTIONS_TO_ADD_PER_BUCKET = "collections_to_add_per_bucket"

    SCOPES_TO_RECREATE = "scopes_to_recreate"
    COLLECTIONS_TO_RECREATE = "collections_to_recreate"

    BUCKET_CONSIDERED_FOR_OPS = "buckets_considered_for_ops"
    SCOPES_CONSIDERED_FOR_OPS = "scopes_considered_for_ops"
    COLLECTIONS_CONSIDERED_FOR_OPS = "collections_considered_for_ops"
    DOC_GEN_TYPE = "doc_gen_type"

    class DocCrud(object):
        NUM_ITEMS_FOR_NEW_COLLECTIONS = "num_items_for_new_collections"

        CREATE_PERCENTAGE_PER_COLLECTION = "create_percentage_per_collection"
        READ_PERCENTAGE_PER_COLLECTION = "read_percentage_per_collection"
        UPDATE_PERCENTAGE_PER_COLLECTION = "update_percentage_per_collection"
        REPLACE_PERCENTAGE_PER_COLLECTION = "replace_percentage_per_collection"
        DELETE_PERCENTAGE_PER_COLLECTION = "delete_percentage_per_collection"
        TOUCH_PERCENTAGE_PER_COLLECTION = "touch_percentage_per_collection"

        # Doc loading options supported
        COMMON_DOC_KEY = "doc_key"
        DOC_KEY_SIZE = "doc_key_size"
        DOC_SIZE = "doc_size"

    class SubDocCrud(object):
        XATTR_TEST = "xattr_test"
        INSERT_PER_COLLECTION = "insert_per_collection"
        UPSERT_PER_COLLECTION = "upsert_per_collection"
        REMOVE_PER_COLLECTION = "remove_per_collection"
        LOOKUP_PER_COLLECTION = "lookup_per_collection"

        # Sub-doc loading option supported
        SUB_DOC_SIZE = "sub_doc_size"

    # Doc_loading task options
    DOC_TTL = "doc_ttl"
    SDK_TIMEOUT = "sdk_timeout"
    SDK_TIMEOUT_UNIT = "sdk_timeout_unit"
    DURABILITY_LEVEL = "durability_level"
    SKIP_READ_ON_ERROR = "skip_read_on_error"
    SUPPRESS_ERROR_TABLE = "suppress_error_table"
    SKIP_READ_SUCCESS_RESULTS = "skip_read_success_results"
    TARGET_VBUCKETS = "target_vbuckets"

    RETRY_EXCEPTIONS = "retry_exceptions"
    IGNORE_EXCEPTIONS = "ignore_exceptions"

    COLLECTIONS_CONSIDERED_FOR_CRUD = "collections_considered_for_crud"
    SCOPES_CONSIDERED_FOR_CRUD = "scopes_considers_for_crud"
    BUCKETS_CONSIDERED_FOR_CRUD = "buckets_considered_for_crud"

    # Number of threadpool executor workers for scope/collection drops/creates
    THREADPOOL_MAX_WORKERS = "threadpool_max_workers"
