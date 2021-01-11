class CbServer(object):
    class Services(object):
        KV = "kv"
        INDEX = "index"
        N1QL = "n1ql"
        CBAS = "cbas"
        FTS = "fts"
        EVENTING = "eventing"
        BACKUP = "backup"

    port = 8091
    capi_port = 8092
    fts_port = 8094
    n1ql_port = 8093
    index_port = 9102
    eventing_port = 8096
    backup_port = 8097

    memcached_port = 11210
    moxi_port = 11211

    default_scope = "_default"
    default_collection = "_default"

    total_vbuckets = 1024

    max_name_len_scope_collection = 251 # max allowed number of bytes for scope/collection name

    # Count excluding the default scope/collection
    max_scopes = 1200
    max_collections = 1200
