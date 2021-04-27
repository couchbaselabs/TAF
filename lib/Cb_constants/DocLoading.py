class Bucket(object):
    class DocOps(object):
        CREATE = "create"
        DELETE = "delete"
        UPDATE = "update"
        REPLACE = "replace"
        READ = "read"
        TOUCH = "touch"

    class SubDocOps(object):
        INSERT = "sd_insert"
        UPSERT = "sd_upsert"
        REMOVE = "sd_remove"
        REPLACE = "sd_replace"
        LOOKUP = "sd_lookup"
        COUNTER = "sd_counter"

    DOC_OPS = [DocOps.DELETE,
               DocOps.CREATE,
               DocOps.UPDATE,
               DocOps.REPLACE,
               DocOps.READ,
               DocOps.TOUCH]
    SUB_DOC_OPS = [SubDocOps.INSERT,
                   SubDocOps.UPSERT,
                   SubDocOps.REMOVE,
                   SubDocOps.LOOKUP]
