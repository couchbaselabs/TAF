class MetaConstants(object):
    NUM_BUCKETS = "num_buckets"
    NUM_SCOPES_PER_BUCKET = "num_scopes_per_bucket"
    NUM_COLLECTIONS_PER_SCOPE = "num_collections_per_scope"
    NUM_ITEMS_PER_COLLECTION = "num_items"
    REMOVE_DEFAULT_COLLECTION = "remove_default_collection"

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

    SCOPES_TO_DROP = "scopes_to_drop"
    SCOPES_TO_ADD_PER_BUCKET = "scopes_to_add_per_bucket"

    COLLECTIONS_TO_ADD_FOR_NEW_SCOPES = "collections_to_add_for_new_scopes"
    COLLECTIONS_TO_ADD_PER_BUCKET = "collections_to_add_per_bucket"

    BUCKET_CONSIDERED_FOR_OPS = "buckets_considered_for_ops"
    SCOPES_CONSIDERED_FOR_OPS = "scopes_considered_for_ops"
    COLLECTIONS_CONSIDERED_FOR_OPS = "collections_considered_for_ops"

    class DocCrud(object):
        NUM_ITEMS_FOR_NEW_COLLECTIONS = "num_items_for_new_collections"

        CREATE_PERCENTAGE_PER_COLLECTION = "create_percentage_per_collection"
        READ_PERCENTAGE_PER_COLLECTION = "read_percentage_per_collection"
        UPDATE_PERCENTAGE_PER_COLLECTION = "update_percentage_per_collection"
        REPLACE_PERCENTAGE_PER_COLLECTION = "replace_percentage_per_collection"
        DELETE_PERCENTAGE_PER_COLLECTION = "delete_percentage_per_collection"

        COMMON_DOC_KEY = "doc_key"
        DOC_KEY_SIZE = "doc_key_size"
        DOC_SIZE = "doc_size"

    class SubDocCrud(object):
        INSERT_PER_COLLECTION = "insert_per_collection"
        UPSERT_PER_COLLECTION = "upsert_per_collection"
        UPDATE_PER_COLLECTION = "update_per_collection"
        REMOVE_PER_COLLECTION = "remove_per_collection"

        SUB_DOC_SIZE = "sub_doc_size"

    COLLECTIONS_CONSIDERED_FOR_CRUD = "collections_considered_for_crud"
    SCOPES_CONSIDERED_FOR_CRUD = "scopes_considers_for_crud"
    BUCKETS_CONSIDERED_FOR_CRUD = "buckets_considered_for_crud"
