from abc import ABCMeta, abstractmethod
from sirius_client_framework import sirius_constants


class DB:
    __metaclass__ = ABCMeta

    def __init__(self, username, password, connection_string, db_type):
        self.username = username
        self.password = password
        self.connection_string = connection_string
        self.db_type = db_type

    @abstractmethod
    def get_parameters(self):
        pass


class CouchbaseLoader(DB):
    def __init__(self,
                 username, password, connection_string,
                 compression_disabled=None,
                 compression_min_size=None,
                 compression_min_ratio=None,
                 connection_timeout=None,
                 kv_timeout=None,
                 kv_durable_timeout=None,
                 bucket=None,
                 scope=None,
                 collection=None,
                 expiry=None,
                 persist_to=None,
                 replicate_to=None,
                 durability=None,
                 operation_timeout=None,
                 cas=None,
                 is_xattr=None,
                 store_semantic=None,
                 preserve_expiry=None,
                 create_path=None,
                 sdk_batch_size=None):
        """
        :param username: Username for authentication. (str)
        :param password: Password for authentication. (str)
        :param connection_string: Connection string for establishing a connection. (str)
        :param compression_disabled: Flag to disable compression. (bool, optional)
        :param compression_min_size: Minimum size for compression. (int, optional)
        :param compression_min_ratio: Minimum compression ratio. (float, optional)
        :param connection_timeout: Timeout for connection establishment. (int, optional)
        :param kv_timeout: Key-Value operation timeout. (int, optional)
        :param kv_durable_timeout: Key-Value durable operation timeout. (int, optional)
        :param bucket: Name of the bucket. (str, optional)
        :param scope: Scope name. (str, optional)
        :param collection: Collection name. (str, optional)
        :param expiry: Expiry time for documents. (int, optional)
        :param persist_to: Number of nodes to persist to. (int, optional)
        :param replicate_to: Number of nodes to replicate to. (int, optional)
        :param durability: Durability requirement. (str, optional)
        :param operation_timeout: Timeout for operations. (int, optional)
        :param cas: Compare-And-Swap value. (int, optional)
        :param is_xattr: Flag indicating whether extended attributes are used. (bool, optional)
        :param store_semantic: Store semantics. (str, optional)
        :param preserve_expiry: Flag to preserve document expiry. (bool, optional)
        :param create_path: Flag to create path. (bool, optional)
        :param sdk_batch_size: Batch size for SDK operations. (int, optional)
        """
        super(CouchbaseLoader, self).__init__(
            username, password, connection_string,
            db_type=sirius_constants.SiriusCodes.DataSources.COUCHBASE)
        self.bucket = bucket
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.durability = durability
        self.scope = scope
        self.collection = collection
        self.preserve_expiry = preserve_expiry
        self.compression_disabled = compression_disabled
        self.compression_min_size = compression_min_size
        self.compression_min_ratio = compression_min_ratio
        self.connection_timeout = connection_timeout
        self.kv_timeout = kv_timeout
        self.kv_durable_timeout = kv_durable_timeout
        self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.expiry = expiry
        self.operation_timeout = operation_timeout
        self.cas = cas
        self.is_xattr = is_xattr
        self.store_semantic = store_semantic
        self.preserve_expiry = preserve_expiry
        self.create_path = create_path
        self.sdk_batch_size = sdk_batch_size

    def get_parameters(self):
        parameter_dict = {}
        if self.compression_disabled is not None:
            parameter_dict['compressionDisabled'] = self.compression_disabled
        if self.compression_min_ratio is not None:
            parameter_dict['compressionMinRatio'] = self.compression_min_ratio
        if self.compression_min_size is not None:
            parameter_dict['compressionMinSize'] = self.compression_min_size
        if self.kv_timeout is not None:
            parameter_dict['KVTimeout'] = self.kv_timeout
        if self.kv_durable_timeout is not None:
            parameter_dict['KVDurableTimeout'] = self.kv_durable_timeout
        if self.bucket is not None:
            parameter_dict['bucket'] = self.bucket
        if self.scope is not None:
            parameter_dict['scope'] = self.scope
        if self.collection is not None:
            parameter_dict['collection'] = self.collection
        if self.persist_to is not None:
            parameter_dict['persistTo'] = self.persist_to
        if self.replicate_to is not None:
            parameter_dict['replicateTo'] = self.replicate_to
        if self.durability is not None:
            parameter_dict['durability'] = self.durability
        if self.operation_timeout is not None:
            parameter_dict['operationTimeout'] = self.operation_timeout
        if self.cas is not None:
            parameter_dict['cas'] = self.cas
        if self.store_semantic is not None:
            parameter_dict['storeSemantic'] = self.store_semantic
        if self.preserve_expiry is not None:
            parameter_dict['preserveExpiry'] = self.preserve_expiry
        if self.create_path is not None:
            parameter_dict['createPath'] = self.create_path
        if self.sdk_batch_size is not None:
            parameter_dict['SDKBatchSize'] = self.sdk_batch_size
        if self.connection_timeout is not None:
            parameter_dict['connectionTimeout'] = self.connection_timeout
        if self.is_xattr is not None:
            parameter_dict['isXattr'] = self.is_xattr
        if self.expiry is not None:
            parameter_dict['expiry'] = self.expiry
        return parameter_dict


class MongoLoader(DB):
    def __init__(self, username, password, connection_string,
                 collection=None, database=None,
                 sdk_batch_size=None):
        """
        :param username: Username for authentication. (str)
        :param password: Password for authentication. (str)
        :param connection_string: Connection string for establishing a connection. (str)
        :param collection: Collection name. (str, optional)
        :param database: Database name. (str, optional)
        :param sdk_batch_size: Batch size for bulk CRUD operations. (int, optional)
        """
        super(MongoLoader, self).__init__(
            username, password, connection_string,
            db_type=sirius_constants.SiriusCodes.DataSources.MONGODB)
        self.collection = collection
        self.database = database
        self.sdk_batch_size = sdk_batch_size

    def get_parameters(self):
        parameter_dict = {}
        if self.collection is not None:
            parameter_dict['collection'] = self.collection
        if self.database is not None:
            parameter_dict['database'] = self.database
        if self.sdk_batch_size is not None:
            parameter_dict['SDKBatchSize'] = self.sdk_batch_size
        return parameter_dict


class CassandraLoader(DB):
    def __init__(self,
                 username, password, connection_string,
                 keyspace=None,
                 table=None,
                 num_of_conns=None,
                 sub_doc_path=None,
                 replication_factor=None,
                 cassandra_class=None,
                 db_on_local=None,
                 sdk_batch_size=None):
        """
        :param username: Username for authentication. (str)
        :param password: Password for authentication. (str)
        :param connection_string: Connection string for establishing a connection. (str)
        :param keyspace: Keyspace name. (str, optional)
        :param table: Table name. (str, optional)
        :param num_of_conns: Number of connections. (int, optional)
        :param sub_doc_path: Sub-document path (field specification) for sub doc CRUD operations. (str, optional)
        :param replication_factor: Replication factor required for keyspace/table creation. (int, optional)
        :param cassandra_class: Cassandra class required for keyspace/table creation. (str, optional)
        :param db_on_local: DB on local host (bool, optional)
        :param sdk_batch_size: Batch size for bulk CRUD operations. (int, optional)
        """
        super(CassandraLoader, self).__init__(username, password, connection_string,
                                              db_type=sirius_constants.SiriusCodes.DataSources.CASSANDRA)
        self.keyspace = keyspace
        self.table = table
        self.num_of_conns = num_of_conns
        self.sub_doc_path = sub_doc_path
        self.replication_factor = replication_factor
        self.cassandra_class = cassandra_class
        self.db_on_local = db_on_local
        self.sdk_batch_size = sdk_batch_size

    def get_parameters(self):
        parameter_dict = {}
        if self.keyspace is not None:
            parameter_dict['keyspace'] = self.keyspace
        if self.table is not None:
            parameter_dict['table'] = self.table
        if self.num_of_conns is not None:
            parameter_dict['numOfConns'] = self.num_of_conns
        if self.sub_doc_path is not None:
            parameter_dict['subDocPath'] = self.sub_doc_path
        if self.replication_factor is not None:
            parameter_dict['replicationFactor'] = self.replication_factor
        if self.cassandra_class is not None:
            parameter_dict['cassandraClass'] = self.cassandra_class
        if self.db_on_local is not None:
            parameter_dict['dbOnLocal'] = self.db_on_local
        if self.sdk_batch_size is not None:
            parameter_dict['SDKBatchSize'] = self.sdk_batch_size
        return parameter_dict


class ColumnarLoader(DB):
    def __init__(self,
                 username, password, connection_string,
                 bucket=None, scope=None, collection=None,
                 query=None,
                 hosted_on_prem=None,
                 sdk_batch_size=None):
        """
        :param username: Username for authentication. (str)
        :param password: Password for authentication. (str)
        :param connection_string: Connection string for establishing a connection. (str)
        :param bucket: Bucket name. (str, optional)
        :param scope: Scope name. (str, optional)
        :param collection: Collection name. (str, optional)
        :param query: N1QL query to use a view as referrence. (str, optional)
        :param hosted_on_prem: Hosted on prem. (bool, optional)
        :param sdk_batch_size: Batch size for bulk CRUD operations. (int, optional)
        """
        super(ColumnarLoader, self).__init__(
            username, password, connection_string,
            db_type=sirius_constants.SiriusCodes.DataSources.COLUMNAR)
        self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.query = query
        self.hosted_on_prem = hosted_on_prem
        self.sdk_batch_size = sdk_batch_size

    def get_parameters(self):
        parameter_dict = {}
        if self.bucket is not None:
            parameter_dict['bucket'] = self.bucket
        if self.scope is not None:
            parameter_dict['scope'] = self.scope
        if self.collection is not None:
            parameter_dict['collection'] = self.collection
        if self.query is not None:
            parameter_dict['query'] = self.query
        if self.hosted_on_prem is not None:
            parameter_dict['hostedOnPrem'] = self.hosted_on_prem
        if self.sdk_batch_size is not None:
            parameter_dict['SDKBatchSize'] = self.sdk_batch_size
        return parameter_dict


class MySQLLoader(DB):
    def __init__(self,
                 username, password, connection_string,
                 database=None,
                 table=None,
                 port=None,
                 max_idle_connections=None,
                 max_open_connections=None,
                 max_idle_time=None,
                 max_life_time=None,
                 sdk_batch_size=None):
        """
        :param username: Username for authentication. (str)
        :param password: Password for authentication. (str)
        :param connection_string: Connection string for establishing a connection. (str)
        :param database: Database name. (str, optional)
        :param table: Table name. (str, optional)
        :param port: Port number. (int, optional)
        :param max_idle_connections: Max idle connections. (int, optional)
        :param max_open_connections: Max open connections. (int, optional)
        :param max_idle_time: Max idle time. (int, optional)
        :param max_life_time: Max life-time. (int, optional)
        :param sdk_batch_size: Batch size for bulk CRUD operations. (int, optional)
        """
        super(MySQLLoader, self).__init__(
            username, password, connection_string,
            db_type=sirius_constants.SiriusCodes.DataSources.MYSQL)
        self.database = database
        self.table = table
        self.port = port
        self.max_idle_connections = max_idle_connections
        self.max_open_connections = max_open_connections
        self.max_idle_time = max_idle_time
        self.max_life_time = max_life_time
        self.sdk_batch_size = sdk_batch_size

    def get_parameters(self):
        parameter_dict = {}
        if self.database is not None:
            parameter_dict['database'] = self.database
        if self.port is not None:
            parameter_dict['port'] = self.port
        if self.max_idle_connections is not None:
            parameter_dict['maxIdleConnections'] = self.max_idle_connections
        if self.max_open_connections is not None:
            parameter_dict['maxOpenConnections'] = self.max_open_connections
        if self.max_idle_time is not None:
            parameter_dict['maxIdleTime'] = self.max_idle_time
        if self.max_life_time is not None:
            parameter_dict['maxLifeTime'] = self.max_life_time
        if self.sdk_batch_size is not None:
            parameter_dict['SDKBatchSize'] = self.sdk_batch_size
        return parameter_dict


class DynamoDBLoader(DB):
    def __init__(self,
                 username, password, connection_string,
                 table=None,
                 provisioned=None,
                 read_capacity=None,
                 write_capacity=None):
        """
        :param username: Username for authentication. (str)
        :param password: Password for authentication. (str)
        :param connection_string: Connection string for establishing a connection. (str)
        :param table: Table name. (str, optional)
        :param provisioned: Provisioned Instance or On-Demand. (int, optional)
        :param read_capacity: Read Capacity. (int, optional)
        :param write_capacity: Write Capacity. (int, optional)
        """
        super(DynamoDBLoader, self).__init__(
            username, password, connection_string,
            db_type=sirius_constants.SiriusCodes.DataSources.DYNAMODB)
        self.table = table
        self.provisioned = provisioned
        self.read_capacity = read_capacity
        self.write_capacity = write_capacity

    def get_parameters(self):
        parameter_dict = {}
        if self.table is not None:
            parameter_dict['table'] = self.table
        if self.provisioned is not None:
            parameter_dict['provisioned'] = self.provisioned
        if self.read_capacity is not None:
            parameter_dict['readCapacity'] = self.read_capacity
        if self.write_capacity is not None:
            parameter_dict['writeCapacity'] = self.write_capacity
        return parameter_dict
