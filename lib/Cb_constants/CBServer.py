class CbServer(object):
    class Services(object):
        KV = "kv"
        INDEX = "index"
        N1QL = "n1ql"
        CBAS = "cbas"
        FTS = "fts"
        EVENTING = "eventing"
        BACKUP = "backup"

        @classmethod
        def services_require_memory(cls):
            return [CbServer.Services.KV,
                    CbServer.Services.INDEX,
                    CbServer.Services.CBAS,
                    CbServer.Services.FTS,
                    CbServer.Services.EVENTING]

    class Settings(object):
        KV_MEM_QUOTA = "memoryQuota"
        INDEX_MEM_QUOTA = "indexMemoryQuota"
        FTS_MEM_QUOTA = "ftsMemoryQuota"
        CBAS_MEM_QUOTA = "cbasMemoryQuota"
        EVENTING_MEM_QUOTA = "eventingMemoryQuota"

        class MinRAMQuota(object):
            KV = 256
            INDEX = 256
            FTS = 256
            EVENTING = 256
            CBAS = 1024

    class Failover(object):
        class Type(object):
            AUTO = "auto"
            GRACEFUL = "graceful"
            FORCEFUL = "forceful"

        class RecoveryType(object):
            FULL = "full"
            DELTA = "delta"

        # Node Availability - Num events
        MIN_EVENTS = 1
        MAX_EVENTS = 100

        # Node Availability - Timeout in seconds
        MIN_TIMEOUT = 5
        MAX_TIMEOUT = 3600

    class Serverless(object):
        KV_SubCluster_Size = 3

        VB_COUNT = 64
        MAX_BUCKETS = 25
        MAX_WEIGHT = 10000

    enterprise_edition = True

    port = 8091
    capi_port = 8092
    fts_port = 8094
    n1ql_port = 8093
    index_port = 9102
    eventing_port = 8096
    backup_port = 8097
    cbas_port = 8095

    ssl_port = 18091
    ssl_capi_port = 18092
    ssl_fts_port = 18094
    ssl_n1ql_port = 18093
    ssl_index_port = 19102
    ssl_eventing_port = 18096
    ssl_backup_port = 18097
    ssl_cbas_port = 18095

    memcached_port = 11210
    ssl_memcached_port = 11207

    # map of {non-ssl,ssl} ports
    ssl_port_map = {str(port): str(ssl_port),
                    str(capi_port): str(ssl_capi_port),
                    str(fts_port): str(ssl_fts_port),
                    str(n1ql_port): str(ssl_n1ql_port),
                    str(index_port): str(ssl_index_port),
                    str(eventing_port): str(ssl_eventing_port),
                    str(cbas_port): str(ssl_cbas_port),
                    str(memcached_port): str(ssl_memcached_port),
                    str(backup_port): str(ssl_backup_port)}
    use_https = False
    n2n_encryption = False

    default_server_group = "Group 1"
    default_scope = "_default"
    default_collection = "_default"

    # This block is valid only for Serverless profile builds
    system_scope = "_system"
    eventing_collection = "_eventing"
    mobile_collection = "_mobile"
    query_collection = "_query"

    total_vbuckets = 1024

    # Name length limits
    max_bucket_name_len = 100
    max_scope_name_len = 251
    max_collection_name_len = 251

    # Count excluding the default scope/collection
    max_scopes = 1200
    max_collections = 1200

    # Max supported system_event_logs
    sys_event_min_logs = 3000
    sys_event_max_logs = 20000
    sys_event_def_logs = 10000
    # Size in bytes
    sys_event_log_max_size = 3072
    # Time within which the UUID cannot be duplicated in server (in seconds)
    sys_event_log_uuid_uniqueness_time = 60

    cluster_profile = "default"
