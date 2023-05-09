class Bucket(object):
    class DocOps(object):
        CREATE = "create"
        DELETE = "delete"
        UPDATE = "update"
        REPLACE = "replace"
        READ = "read"
        TOUCH = "touch"
        VALIDATE = "validate"
        SINGLE_VALIDATE = "single_validate"

    class SubDocOps(object):
        INSERT = "insert"
        UPSERT = "upsert"
        REMOVE = "remove"
        LOOKUP = "lookup"
        REPLACE = "replace"
        COUNTER = "counter"
        VALIDATE = "validate"

    DOC_OPS = [DocOps.DELETE,
               DocOps.READ,
               DocOps.TOUCH,
               DocOps.UPDATE,
               DocOps.REPLACE,
               DocOps.CREATE]
    SUB_DOC_OPS = [SubDocOps.REMOVE,
                   SubDocOps.LOOKUP,
                   SubDocOps.UPSERT,
                   SubDocOps.INSERT]
