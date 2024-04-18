class SiriusCodes(object):
    class Providers(object):
        AWS = "awsS3"

    class Templates(object):
        PERSON = "person"
        HOTEL = "hotel"
        PRODUCT = "product"
        SMALL = "small"
        ECOMMERCE = "ecommerce"
        PERSON_SQL = "person_sql"
        HOTEL_SQL = "hotel_sql"
        PRODUCT_SQL = "product_sql"
        SMALL_SQL = "small_sql"

    class DataSources(object):
        MONGODB = "mongodb"
        CASSANDRA = "cassandra"
        MYSQL = "mysql"
        DYNAMODB = "dynamodb"
        COUCHBASE = "couchbase"
        COLUMNAR = "columnar"

    class DocOps(object):
        CREATE = "create"
        DELETE = "delete"
        UPDATE = "update"
        REPLACE = "replace"
        READ = "read"
        TOUCH = "touch"
        BULK_CREATE = "bulkCreate"
        BULK_DELETE = "bulkDelete"
        BULK_UPDATE = "bulkUpdate"
        BULK_REPLACE = "bulkReplace"
        BULK_READ = "bulkRead"
        BULK_TOUCH = "bulkTouch"
        VALIDATE_COLUMNAR = "validateColumnar"
        RETRY_EXCEPTIONS = "retryExceptions"
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
        CREATE_DIR = "createFolderS3Doc"
        DELETE_DIR = "deleteFolderS3Doc"
        CREATE = "createS3Doc"
        DELETE = "deleteS3Doc"
        UPDATE = "updateS3Doc"
        LOAD = "loadS3Doc"
        UPDATE_FILES = "updateFilesS3Doc"
        DELETE_FILES = "deleteFilesS3Doc"

    DOC_OPS = [
        DocOps.DELETE,
        DocOps.READ,
        DocOps.TOUCH,
        DocOps.UPDATE,
        DocOps.REPLACE,
        DocOps.CREATE,
        DocOps.BULK_CREATE,
        DocOps.BULK_TOUCH,
        DocOps.BULK_DELETE,
        DocOps.BULK_READ,
        DocOps.BULK_REPLACE,
        DocOps.BULK_UPDATE,
        DocOps.VALIDATE,
        DocOps.VALIDATE_COLUMNAR,
        DocOps.RETRY_EXCEPTIONS,
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
        S3DocOps.CREATE_DIR,
        S3DocOps.DELETE_DIR,
        S3DocOps.LOAD,
        S3DocOps.UPDATE,
        S3DocOps.DELETE_FILES,
        S3DocOps.UPDATE_FILES,
    ]

    S3_MGMT_OPS = [S3MgmtOps.INFO, S3MgmtOps.CREATE, S3MgmtOps.DELETE]


WORKLOAD_PATH = {
    SiriusCodes.DocOps.CREATE: "/create",
    SiriusCodes.DocOps.BULK_CREATE: "/bulk-create",
    SiriusCodes.DocOps.UPDATE: "/upsert",
    SiriusCodes.DocOps.BULK_UPDATE: "/bulk-upsert",
    SiriusCodes.DocOps.READ: "/read",
    SiriusCodes.DocOps.BULK_READ: "/bulk-read",
    SiriusCodes.SubDocOps.INSERT: "single",
    SiriusCodes.SubDocOps.UPSERT: "/sub-doc-upsert",
    SiriusCodes.SubDocOps.LOOKUP: "/sub-doc-read",
    SiriusCodes.SubDocOps.REMOVE: "/sub-doc-delete",
    SiriusCodes.DocOps.DELETE: "/delete",
    SiriusCodes.DocOps.BULK_DELETE: "/bulk-delete",
    SiriusCodes.DocOps.TOUCH: "/touch",
    SiriusCodes.DocOps.BULK_TOUCH: "/bulk-touch",
    SiriusCodes.DocOps.VALIDATE_COLUMNAR: "/validate-columnar",
    SiriusCodes.DocOps.RETRY_EXCEPTIONS: "/retry-exceptions",
    SiriusCodes.DocOps.VALIDATE: "/validate",
}

DB_MGMT_PATH = {
    SiriusCodes.DBMgmtOps.CREATE: "/create-database",
    SiriusCodes.DBMgmtOps.DELETE: "/delete-database",
    SiriusCodes.DBMgmtOps.LIST: "/list-database",
    SiriusCodes.DBMgmtOps.COUNT: "/count",
    SiriusCodes.DBMgmtOps.WARMUP: "/warmup-bucket",
    SiriusCodes.DBMgmtOps.CLEAR: "/clear_data",
}

BLOB_PATH = {
    SiriusCodes.S3MgmtOps.CREATE: "/create-bucket",
    SiriusCodes.S3MgmtOps.DELETE: "/delete-bucket",
    SiriusCodes.S3MgmtOps.INFO: "/get-info",
    SiriusCodes.S3DocOps.CREATE_DIR: "/create-folder",
    SiriusCodes.S3DocOps.DELETE_DIR: "/delete-folder",
    SiriusCodes.S3DocOps.CREATE: "/create-file",
    SiriusCodes.S3DocOps.UPDATE: "/update-file",
    SiriusCodes.S3DocOps.DELETE: "/delete-file",
    SiriusCodes.S3DocOps.LOAD: "/create-files-in-folders",
    SiriusCodes.S3DocOps.UPDATE_FILES: "/update-files-in-folder",
    SiriusCodes.S3DocOps.DELETE_FILES: "/delete-files-in-folder",
}
