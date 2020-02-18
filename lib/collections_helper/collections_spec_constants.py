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
