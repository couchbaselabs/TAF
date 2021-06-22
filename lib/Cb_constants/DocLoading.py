class Bucket(object):
    class DocOps(object):
        CREATE = "create"
        DELETE = "delete"
        UPDATE = "update"
        REPLACE = "replace"
        READ = "read"
        TOUCH = "touch"

    class SubDocOps(object):
        INSERT = "insert"
        UPSERT = "upsert"
        REMOVE = "remove"
        LOOKUP = "lookup"
        COUNTER = "counter"

    DOC_OPS = [DocOps.DELETE,
               DocOps.CREATE,
               DocOps.UPDATE,
               DocOps.REPLACE,
               DocOps.READ,
               DocOps.TOUCH]
    SUB_DOC_OPS = [SubDocOps.REMOVE,
                   SubDocOps.LOOKUP,
                   SubDocOps.INSERT,
                   SubDocOps.UPSERT]
