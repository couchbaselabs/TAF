class DocOpCodes(object):

    class DocOps(object):
        CREATE = "create"
        DELETE = "delete"
        UPDATE = "update"
        REPLACE = "replace"
        READ = "read"
        TOUCH = "touch"
        BULKCREATE = "bulkCreate"
        BULKDELETE = "bulkDelete"
        BULKUPDATE = "bulkUpdate"
        BULKREPLACE = "bulkReplace"
        BULKREAD = "bulkRead"
        BULKTOUCH = "bulkTouch"
        VALIDATECOLUMNAR = "validateColumnar"
        RETRY = "retryExceptions"
        VALIDATE = "validate"

    class DBMgmtOps(object):
        CREATE = "createDB"
        DELETE = "deleteDB"
        LIST = "listDB"
        COUNT = "countDB"
        WARMUP = "warmupDB"
        CLEAR = "clearDB"

    class SubDocOps(object):
        INSERT = "insertSubDoc"
        UPSERT = "upsertSubDoc"
        REMOVE = "removeSubDoc"
        LOOKUP = "lookupSubDoc"
        COUNTER = "counterSubDoc"

    class S3MgmtOps(object):
        CREATE = "createS3"
        DELETE = "deleteS3"
        INFO = "infoS3"

    class S3DocOps(object):
        CREATEDIR = "createFolderS3Doc"
        DELETEDIR = "deleteFolderS3Doc"
        CREATE = "createS3Doc"
        DELETE = "deleteS3Doc"
        UPDATE = "updateS3Doc"
        LOAD = "loadS3Doc"
        UPDATEFILES = "updateFilesS3Doc"
        DELETEFILES = "deleteFilesS3Doc"

    DOC_OPS = [
        DocOps.DELETE,
        DocOps.READ,
        DocOps.TOUCH,
        DocOps.UPDATE,
        DocOps.REPLACE,
        DocOps.CREATE,
        DocOps.BULKCREATE,
        DocOps.BULKTOUCH,
        DocOps.BULKDELETE,
        DocOps.BULKREAD,
        DocOps.BULKREPLACE,
        DocOps.BULKUPDATE,
        DocOps.VALIDATE,
        DocOps.VALIDATECOLUMNAR,
        DocOps.RETRY,
    ]

    SUB_DOC_OPS = [
        SubDocOps.REMOVE,
        SubDocOps.LOOKUP,
        SubDocOps.UPSERT,
        SubDocOps.INSERT,
    ]

    DB_MGMT_OPS = [
        DBMgmtOps.CREATE,
        DBMgmtOps.CLEAR,
        DBMgmtOps.COUNT,
        DBMgmtOps.DELETE,
        DBMgmtOps.LIST,
        DBMgmtOps.WARMUP,
    ]

    S3_DOC_OPS = [
        S3DocOps.DELETE,
        S3DocOps.CREATE,
        S3DocOps.CREATEDIR,
        S3DocOps.DELETEDIR,
        S3DocOps.LOAD,
        S3DocOps.UPDATE,
        S3DocOps.DELETEFILES,
        S3DocOps.UPDATEFILES,
    ]

    S3_MGMT_OPS = [S3MgmtOps.INFO, S3MgmtOps.CREATE, S3MgmtOps.DELETE]
