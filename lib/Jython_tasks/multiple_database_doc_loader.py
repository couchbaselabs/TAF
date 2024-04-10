import traceback

from constants.cloud_constants import DocLoading
from Jython_tasks.task import Task
from doc_loader.sirius_client import RESTClient
from pytests.sdk_workloads.go_workload import IDENTIFIER_TOKEN


class RestBasedLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        db_type,
        username=None,
        password=None,
        connection_string=None,
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
        sdk_batch_size=None,
        database=None,
        query=None,
        columnar_conn_str=None,
        columnar_username=None,
        columnar_password=None,
        columnar_bucket=None,
        columnar_scope=None,
        columnar_collection=None,
        provisioned=None,
        read_capacity=None,
        write_capacity=None,
        keyspace=None,
        table=None,
        num_of_conns=None,
        sub_doc_path=None,
        replication_factor=None,
        cassandra_class=None,
        port=None,
        max_idle_connections=None,
        max_open_connections=None,
        max_idle_time=None,
        max_life_time=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        fieldsToChange=None,
        hostedOnPrem=None,
        dbOnLocal=None,
        aws_access_key=None,
        aws_secret_key=None,
        aws_session_token=None,
        aws_region=None,
        folder_path=None,
        file_format=None,
        file_path=None,
        num_folders=None,
        folders_per_depth=None,
        files_per_folder=None,
        folder_level_names=None,
        max_folder_depth=None,
        min_file_size=None,
        max_file_size=None,
    ):
        self.thread_name = "Sirius_Based_Load_Task_%s_%s".format(db_type, op_type)
        super(RestBasedLoader, self).__init__(self.thread_name)
        self.task_manager = task_manager
        self.generator = generator
        self.op_type = op_type
        self.db_type = db_type
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
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.durability = durability
        self.operation_timeout = operation_timeout
        self.cas = cas
        self.is_xattr = is_xattr
        self.store_semantic = store_semantic
        self.preserve_expiry = preserve_expiry
        self.create_path = create_path
        self.sdk_batch_size = sdk_batch_size
        self.database = database
        self.query = query
        self.columnar_conn_str = columnar_conn_str
        self.columnar_username = columnar_username
        self.columnar_password = columnar_password
        self.columnar_bucket = columnar_bucket
        self.columnar_scope = columnar_scope
        self.columnar_collection = columnar_collection
        self.provisioned = provisioned
        self.read_capacity = read_capacity
        self.write_capacity = write_capacity
        self.keyspace = keyspace
        self.table = table
        self.num_of_conns = num_of_conns
        self.sub_doc_path = sub_doc_path
        self.replication_factor = replication_factor
        self.cassandra_class = cassandra_class
        self.port = port
        self.max_idle_connections = max_idle_connections
        self.max_open_connections = max_open_connections
        self.max_idle_time = max_idle_time
        self.max_life_time = max_life_time
        self.doc_op_iterations_done = 0
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.identifier_token = IDENTIFIER_TOKEN
        self.username = username
        self.password = password
        self.connection_string = connection_string
        self.columnar_username = columnar_username
        self.columnar_password = columnar_password
        self.columnar_conn_str = columnar_conn_str
        self.start = int(self.generator.start)
        self.end = int(self.generator.end)
        self.doc_type = self.generator.doc_type
        self.key_prefix = self.generator.name
        self.key_suffix = ""
        self.count = self.end - self.start
        self.doc_size = self.generator.doc_size
        self.key_size = self.generator.key_size
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False
        self.fieldsToChange = fieldsToChange
        self.hosted_on_prem = hostedOnPrem
        self.dbOnLocal = dbOnLocal
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_session_token = aws_session_token
        self.aws_region = aws_region
        self.folder_path = folder_path
        self.file_format = file_format
        self.file_path = file_path
        self.folders_per_depth = folders_per_depth
        self.files_per_folder = files_per_folder
        self.folder_level_names = folder_level_names
        self.max_folder_depth = max_folder_depth
        self.min_file_size = min_file_size
        self.max_file_size = max_file_size
        self.num_folders = num_folders

    def call(self):
        try:
            self.start_task()
            sirius_client_object = RESTClient(
                sirius_base_url=self.sirius_base_url,
            )
            extra = sirius_client_object.build_rest_payload_extra(
                compressionDisabled=self.compression_disabled,
                compressionMinSize=self.compression_min_size,
                compressionMinRatio=self.compression_min_ratio,
                connectionTimeout=self.connection_timeout,
                KVTimeout=self.kv_timeout,
                KVDurableTimeout=self.kv_durable_timeout,
                bucket=self.bucket,
                scope=self.scope,
                collection=self.collection,
                expiry=self.expiry,
                persistTo=self.persist_to,
                replicateTo=self.replicate_to,
                durability=self.durability,
                operationTimeout=self.operation_timeout,
                cas=self.cas,
                isXattr=self.is_xattr,
                storeSemantic=self.store_semantic,
                preserveExpiry=self.preserve_expiry,
                createPath=self.create_path,
                SDKBatchSize=self.sdk_batch_size,
                database=self.database,
                query=self.query,
                connStr=self.columnar_conn_str,
                username=self.columnar_username,
                password=self.columnar_password,
                columnarBucket=self.columnar_bucket,
                columnarScope=self.columnar_scope,
                columnarCollection=self.columnar_collection,
                provisioned=self.provisioned,
                readCapacity=self.read_capacity,
                writeCapacity=self.write_capacity,
                keyspace=self.keyspace,
                table=self.table,
                numOfConns=self.num_of_conns,
                subDocPath=self.sub_doc_path,
                replicationFactor=self.replication_factor,
                cassandraClass=self.cassandra_class,
                port=self.port,
                maxIdleConnections=self.max_idle_connections,
                maxOpenConnections=self.max_open_connections,
                maxIdleTime=self.max_idle_time,
                maxLifeTime=self.max_life_time,
                hostedOnPrem=self.hosted_on_prem,
                dbOnLocal=self.dbOnLocal,
            )

            operation_config = sirius_client_object.build_rest_payload_operation_config(
                start=self.start,
                end=self.end,
                docSize=self.doc_size,
                template=self.template,
                fieldToChange=self.fieldsToChange,
            )

            if (
                self.op_type in DocLoading.DocOpCodes.DOC_OPS
                or self.op_type in DocLoading.DocOpCodes.SUB_DOC_OPS
            ):
                self.fail, self.success_count, self.fail_count, self.resultSeed = (
                    sirius_client_object.do_data_loading(
                        op_type=self.op_type,
                        db_type=self.db_type,
                        identifier_token=self.identifier_token,
                        username=self.username,
                        password=self.username,
                        connection_string=self.connection_string,
                        extra=extra,
                        operation_config=operation_config,
                    )
                )
                print("Fail: ", self.fail, "Success: ", self.success_count, "Fail Count: ", self.fail_count, "ResultSeed: ", self.resultSeed)

            if self.op_type in DocLoading.DocOpCodes.DB_MGMT_OPS:
                data = sirius_client_object.do_db_mgmt(
                    op_type=self.op_type,
                    db_type=self.db_type,
                    connection_string=self.connection_string,
                    username=self.username,
                    password=self.password,
                    operation_config=operation_config,
                    extra=extra,
                    identifier_token=self.identifier_token,
                )
                if not data.get("error"):
                    self.message = data.get("message")
                    self.data = data.get("data")
                    print("Message:", self.message, "Data", self.data)

            if self.op_type in DocLoading.DocOpCodes.S3_DOC_OPS or self.op_type in DocLoading.DocOpCodes.S3_MGMT_OPS:
                externalStorageConfig = (
                    sirius_client_object.build_rest_payload_s3_external_storage_extras(
                        awsAccessKey=self.aws_access_key,
                        awsSecretKey=self.aws_secret_key,
                        awsRegion=self.aws_region,
                        bucket=self.bucket,
                        awsSessionToken=self.aws_session_token,
                        folderPath=self.folder_path,
                        fileFormat=self.file_format,
                        filePath=self.file_path,
                        numFolders=self.num_folders,
                        foldersPerDepth=self.folders_per_depth,
                        filesPerFolder=self.files_per_folder,
                        folderLevelNames=self.folder_level_names,
                        maxFolderDepth=self.max_folder_depth,
                        minFileSize=self.min_file_size,
                        maxFileSize=self.max_file_size,
                    )
                )
                self.fail, self.success_count, self.fail_count, self.resultSeed = (
                    sirius_client_object.do_blob_loading(
                        op_type=self.op_type,
                        external_storage_extras=externalStorageConfig,
                        identifier_token=self.identifier_token,
                        extra=extra,
                        operation_config=operation_config,
                    )
                )
                print("Fail: ", self.fail, "Success: ", self.success_count, "Fail Count: ", self.fail_count, "ResultSeed: ", self.resultSeed)

            if self.op_type == "validate" or self.op_type == "validateColumnar":
                if len(self.fail) > 0:
                    for key, failed_doc in self.fail:
                        print(key, failed_doc)

                    self.set_result(True)
                    self.complete_task()
                    raise Exception("Validation Failed")

            self.set_result(True)
            self.complete_task()
        except Exception as e:
            self.log.debug((traceback.format_exc()))
            self.log.error(str(e))
            raise e
        self.set_result(True)
        self.complete_task()
        return True
    def get_total_doc_ops(self):
        return self.end - self.start

class CouchbaseDocLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        username,
        password,
        connection_string,
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
        sdk_batch_size=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        fieldsToChange=None,
        print_ops_rate=True,
        monitor_stats=("doc_ops",),
    ):
        self.thread_name = "Sirius_Couchbase_Load_Task_%s".format( op_type )
        super(CouchbaseDocLoader, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = "couchbase"
        self.bucket = None
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.task_manager = task_manager
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.buckets = None
        self.print_ops_rate = print_ops_rate
        self.durability = durability
        self.monitor_stats = monitor_stats
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
        self.doc_op_iterations_done = 0
        self.operation_timeout = operation_timeout
        self.cas = cas
        self.is_xattr = is_xattr
        self.store_semantic = store_semantic
        self.preserve_expiry = preserve_expiry
        self.create_path = create_path
        self.sdk_batch_size = sdk_batch_size
        self.username = username
        self.password = password
        self.connection_string = connection_string
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_types = [op_type]
        if isinstance(bucket, list):
            self.buckets = bucket
        else:
            self.buckets = [bucket]
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.fieldsToChange = fieldsToChange
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False

    def call(self):
        try:
            self.start_task()
            buckets_for_ops_rate_task = list()
            if self.op_types:
                if len(self.op_types) != len(self.generators):
                    self.set_exception(Exception("Not all generators have op_type!"))
                    self.complete_task()
            if self.buckets:
                if len(self.op_types) != len(self.buckets):
                    self.set_exception(
                        Exception("Not all generators have bucket specified!")
                    )
                    self.complete_task()
            iterator = 0
            for generator in self.generators:
                if self.op_types:
                    self.op_type = self.op_types[iterator]
                if self.buckets:
                    self.bucket = self.buckets[iterator]
                self.__tasks.append(
                    RestBasedLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        username=self.username,
                        password=self.password,
                        connection_string=self.connection_string,
                        compression_disabled=self.compression_disabled,
                        compression_min_size=self.compression_min_size,
                        compression_min_ratio=self.compression_min_ratio,
                        connection_timeout=self.connection_timeout,
                        kv_timeout=self.kv_timeout,
                        kv_durable_timeout=self.kv_durable_timeout,
                        bucket=self.bucket,
                        scope=self.scope,
                        collection=self.collection,
                        expiry=self.expiry,
                        persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        durability=self.durability,
                        operation_timeout=self.operation_timeout,
                        cas=self.cas,
                        is_xattr=self.is_xattr,
                        store_semantic=self.store_semantic,
                        preserve_expiry=self.preserve_expiry,
                        create_path=self.create_path,
                        sdk_batch_size=self.sdk_batch_size,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                        fieldsToChange=self.fieldsToChange,
                    )
                )
                iterator += 1
            if self.print_ops_rate:
                if self.buckets:
                    buckets_for_ops_rate_task = self.buckets
                else:
                    buckets_for_ops_rate_task = [self.bucket]
                for bucket in buckets_for_ops_rate_task:
                    bucket.stats.manage_task(
                        "start",
                        self.task_manager,
                        cluster=self.cluster,
                        bucket=bucket,
                        monitor_stats=self.monitor_stats,
                        sleep=1,
                    )
            try:
                for task in self.__tasks:
                    self.task_manager.add_new_task(task)
                for task in self.__tasks:
                    try:
                        self.task_manager.get_task_result(task)
                        self.log.debug(
                            "{0} - Items loaded {1}".format(
                                task.thread_name, task.get_total_doc_ops()
                            )
                        )
                    except Exception as e:
                        self.test_log.error(e)
                    finally:
                        self.fail.update(task.fail)
                        self.success.update(task.success)
                        self.fail_count += task.fail_count
                        self.success_count += task.success_count
                        self.resultSeed = task.resultSeed
                        self.log.warning(
                            "Failed to load {0} sub_docs from {1} to {2}".format(
                                task.fail.__len__(),
                                task.generator.start,
                                task.generator.end,
                            )
                        )
            except Exception as e:
                self.test_log.error(e)
                self.set_exception(e)
            finally:
                if self.print_ops_rate:
                    for bucket in buckets_for_ops_rate_task:
                        bucket.stats.manage_task("stop", self.task_manager)
                self.log.debug("========= Tasks in loadgen pool=======")
                self.task_manager.print_tasks_in_pool()
                self.log.debug("======================================")
                for task in self.__tasks:
                    self.task_manager.stop_task(task)
                    self.log.debug(
                        "Task '{0}' complete. Loaded {1} items".format(
                            task.thread_name, task.get_total_doc_ops
                        )
                    )
        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        self.complete_task()
        return self.fail

    def end_task(self):
        self.log.debug("%s - Stopping load" % self.thread_name)
        for task in self.__tasks:
            task.end_task()

    def get_total_doc_ops(self):
        total_ops = 0
        for sub_task in self.__tasks:
            total_ops += sub_task.get_total_doc_ops()
        return total_ops

class MongoDocLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        username,
        password,
        connection_string,
        collection=None,
        create_path=None,
        sdk_batch_size=None,
        database=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        fieldsToChange=None,
    ):

        self.thread_name = "Sirius_Mongo_Load_Task_%s".format( op_type )
        super(MongoDocLoader, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = "mongodb"
        self.bucket = None
        self.task_manager = task_manager
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.collection = collection
        self.collection = collection
        self.doc_op_iterations_done = 0
        self.create_path = create_path
        self.sdk_batch_size = sdk_batch_size
        self.database = database
        self.identifier_token = IDENTIFIER_TOKEN
        self.username = username
        self.password = password
        self.connection_string = connection_string
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_types = [op_type]
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.fieldsToChange = fieldsToChange
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False
    def call(self):
        try:
            self.start_task()
            if self.op_types:
                if len(self.op_types) != len(self.generators):
                    self.set_exception(Exception("Not all generators have op_type!"))
                    self.complete_task()
            iterator = 0
            for generator in self.generators:
                if self.op_types:
                    self.op_type = self.op_types[iterator]
                self.__tasks.append(
                    RestBasedLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        username=self.username,
                        password=self.password,
                        connection_string=self.connection_string,
                        collection=self.collection,
                        database=self.database,
                        create_path=self.create_path,
                        sdk_batch_size=self.sdk_batch_size,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                        fieldsToChange=self.fieldsToChange,
                    )
                )
                iterator += 1
            try:
                for task in self.__tasks:
                    self.task_manager.add_new_task(task)
                for task in self.__tasks:
                    try:
                        self.task_manager.get_task_result(task)
                        self.log.debug(
                            "{0} - Items loaded {1}".format(
                                task.thread_name, task.get_total_doc_ops()
                            )
                        )
                    except Exception as e:
                        self.test_log.error(e)
                    finally:
                        self.fail.update(task.fail)
                        self.success.update(task.success)
                        self.fail_count += task.fail_count
                        self.success_count += task.success_count
                        self.resultSeed = task.resultSeed
                        self.log.warning(
                            "Failed to load {0} sub_docs from {1} to {2}".format(
                                task.fail.__len__(),
                                task.generator.start,
                                task.generator.end,
                            )
                        )
            except Exception as e:
                self.test_log.error(e)
                self.set_exception(e)
            finally:
                self.log.debug("========= Tasks in loadgen pool=======")
                self.task_manager.print_tasks_in_pool()
                self.log.debug("======================================")
                for task in self.__tasks:
                    self.task_manager.stop_task(task)
                    self.log.debug(
                        "Task '{0}' complete. Loaded {1} items".format(
                            task.thread_name, task.get_total_doc_ops
                        )
                    )

        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        self.complete_task()
        return self.fail

    def end_task(self):
        self.log.debug("%s - Stopping load" % self.thread_name)
        for task in self.__tasks:
            task.end_task()

    def get_total_doc_ops(self):
        total_ops = 0
        for sub_task in self.__tasks:
            total_ops += sub_task.get_total_doc_ops()
        return total_ops

class ColumnarDocLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        username,
        password,
        connection_string,
        bucket=None,
        scope=None,
        collection=None,
        create_path=None,
        sdk_batch_size=None,
        query=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        fieldsToChange=None,
        hostedOnPrem=None,
    ):

        self.thread_name = "Sirius_Columnar_Load_Task_%s".format(op_type)
        super(ColumnarDocLoader, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = "columnar"
        self.bucket = None
        self.task_manager = task_manager
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.buckets = None
        self.scope = scope
        self.collection = collection
        self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.doc_op_iterations_done = 0
        self.create_path = create_path
        self.sdk_batch_size = sdk_batch_size
        self.query = query
        self.identifier_token = IDENTIFIER_TOKEN
        self.username = username
        self.password = password
        self.connection_string = connection_string
        self.hosted_on_prem = hostedOnPrem
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_types = [op_type]
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.fieldsToChange = fieldsToChange
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False

    def call(self):
        try:
            self.start_task()
            if self.op_types:
                if len(self.op_types) != len(self.generators):
                    self.set_exception(Exception("Not all generators have op_type!"))
                    self.complete_task()
            iterator = 0
            for generator in self.generators:
                if self.op_types:
                    self.op_type = self.op_types[iterator]
                self.__tasks.append(
                    RestBasedLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        username=self.username,
                        password=self.password,
                        connection_string=self.connection_string,
                        bucket=self.bucket,
                        scope=self.scope,
                        collection=self.collection,
                        create_path=self.create_path,
                        sdk_batch_size=self.sdk_batch_size,
                        query=self.query,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                        fieldsToChange=self.fieldsToChange,
                        hostedOnPrem=self.hosted_on_prem,
                    )
                )
                iterator += 1
            try:
                for task in self.__tasks:
                    self.task_manager.add_new_task(task)
                for task in self.__tasks:
                    try:
                        self.task_manager.get_task_result(task)
                        self.log.debug(
                            "{0} - Items loaded {1}".format(
                                task.thread_name, task.get_total_doc_ops()
                            )
                        )
                    except Exception as e:
                        self.test_log.error(e)
                    finally:
                        self.fail.update(task.fail)
                        self.success.update(task.success)
                        self.fail_count += task.fail_count
                        self.success_count += task.success_count
                        self.resultSeed = task.resultSeed
                        self.log.warning(
                            "Failed to load {0} sub_docs from {1} to {2}".format(
                                task.fail.__len__(),
                                task.generator.start,
                                task.generator.end,
                            )
                        )
            except Exception as e:
                self.test_log.error(e)
                self.set_exception(e)
            finally:
                self.log.debug("========= Tasks in loadgen pool=======")
                self.task_manager.print_tasks_in_pool()
                self.log.debug("======================================")
                for task in self.__tasks:
                    self.task_manager.stop_task(task)
                    self.log.debug(
                        "Task '{0}' complete. Loaded {1} items".format(
                            task.thread_name, task.get_total_doc_ops
                        )
                    )

        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        self.complete_task()
        return self.fail

    def end_task(self):
        self.log.debug("%s - Stopping load" % self.thread_name)
        for task in self.__tasks:
            task.end_task()

    def get_total_doc_ops(self):
        total_ops = 0
        for sub_task in self.__tasks:
            total_ops += sub_task.get_total_doc_ops()
        return total_ops

class CassandraDocLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        username,
        password,
        connection_string,
        create_path=None,
        sdk_batch_size=None,
        keyspace=None,
        table=None,
        num_of_conns=None,
        sub_doc_path=None,
        replication_factor=None,
        cassandra_class=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        fieldsToChange=None,
        dbOnLocal=None,
    ):

        self.thread_name = "Sirius_Cassandra_Load_Task_%s".format( op_type )
        super(CassandraDocLoader, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = "cassandra"
        self.bucket = None
        self.task_manager = task_manager
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.doc_op_iterations_done = 0
        self.create_path = create_path
        self.sdk_batch_size = sdk_batch_size
        self.keyspace = keyspace
        self.table = table
        self.num_of_conns = num_of_conns
        self.sub_doc_path = sub_doc_path
        self.replication_factor = replication_factor
        self.cassandra_class = cassandra_class
        self.identifier_token = IDENTIFIER_TOKEN
        self.username = username
        self.password = password
        self.connection_string = connection_string
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_types = [op_type]
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.fieldsToChange = fieldsToChange
        self.dbOnLocal = dbOnLocal
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False
    def call(self):
        try:
            self.start_task()
            if self.op_types:
                if len(self.op_types) != len(self.generators):
                    self.set_exception(Exception("Not all generators have op_type!"))
                    self.complete_task()
            iterator = 0
            for generator in self.generators:
                if self.op_types:
                    self.op_type = self.op_types[iterator]
                self.__tasks.append(
                    RestBasedLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        username=self.username,
                        password=self.password,
                        connection_string=self.connection_string,
                        create_path=self.create_path,
                        sdk_batch_size=self.sdk_batch_size,
                        keyspace=self.keyspace,
                        table=self.table,
                        num_of_conns=self.num_of_conns,
                        sub_doc_path=self.sub_doc_path,
                        replication_factor=self.replication_factor,
                        cassandra_class=self.cassandra_class,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                        fieldsToChange=self.fieldsToChange,
                        dbOnLocal=self.dbOnLocal,
                    )
                )
                iterator += 1
            try:
                for task in self.__tasks:
                    self.task_manager.add_new_task(task)
                for task in self.__tasks:
                    try:
                        self.task_manager.get_task_result(task)
                        self.log.debug(
                            "{0} - Items loaded {1}".format(
                                task.thread_name, task.get_total_doc_ops()
                            )
                        )
                    except Exception as e:
                        self.test_log.error(e)
                    finally:
                        self.fail.update(task.fail)
                        self.success.update(task.success)
                        self.fail_count += task.fail_count
                        self.success_count += task.success_count
                        self.resultSeed = task.resultSeed
                        self.log.warning(
                            "Failed to load {0} sub_docs from {1} to {2}".format(
                                task.fail.__len__(),
                                task.generator.start,
                                task.generator.end,
                            )
                        )
            except Exception as e:
                self.test_log.error(e)
                self.set_exception(e)
            finally:
                self.log.debug("========= Tasks in loadgen pool=======")
                self.task_manager.print_tasks_in_pool()
                self.log.debug("======================================")
                for task in self.__tasks:
                    self.task_manager.stop_task(task)
                    self.log.debug(
                        "Task '{0}' complete. Loaded {1} items".format(
                            task.thread_name, task.get_total_doc_ops
                        )
                    )

        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        self.complete_task()
        return self.fail

    def end_task(self):
        self.log.debug("%s - Stopping load" % self.thread_name)
        for task in self.__tasks:
            task.end_task()

    def get_total_doc_ops(self):
        total_ops = 0
        for sub_task in self.__tasks:
            total_ops += sub_task.get_total_doc_ops()
        return total_ops

class MysqlDocLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        username,
        password,
        connection_string,
        create_path=None,
        sdk_batch_size=None,
        database=None,
        table=None,
        port=None,
        max_idle_connections=None,
        max_open_connections=None,
        max_idle_time=None,
        max_life_time=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        fieldsToChange=None,
    ):

        self.thread_name = "Sirius_Mysql_Load_Task_%s".format( op_type )
        super(MysqlDocLoader, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = "mysql"
        self.bucket = None
        self.task_manager = task_manager
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.doc_op_iterations_done = 0
        self.create_path = create_path
        self.sdk_batch_size = sdk_batch_size
        self.database = database
        self.table = table
        self.port = port
        self.max_idle_connections = max_idle_connections
        self.max_open_connections = max_open_connections
        self.max_idle_time = max_idle_time
        self.max_life_time = max_life_time
        self.identifier_token = IDENTIFIER_TOKEN
        self.username = username
        self.password = password
        self.connection_string = connection_string
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_types = [op_type]
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.fieldsToChange = fieldsToChange
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False
    def call(self):
        try:
            self.start_task()
            if self.op_types:
                if len(self.op_types) != len(self.generators):
                    self.set_exception(Exception("Not all generators have op_type!"))
                    self.complete_task()
            iterator = 0
            for generator in self.generators:
                if self.op_types:
                    self.op_type = self.op_types[iterator]
                self.__tasks.append(
                    RestBasedLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        username=self.username,
                        password=self.password,
                        connection_string=self.connection_string,
                        create_path=self.create_path,
                        sdk_batch_size=self.sdk_batch_size,
                        database=self.database,
                        table=self.table,
                        port=self.port,
                        max_idle_connections=self.max_idle_connections,
                        max_open_connections=self.max_open_connections,
                        max_idle_time=self.max_idle_time,
                        max_life_time=self.max_life_time,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                        fieldsToChange=self.fieldsToChange,
                    )
                )
                iterator += 1
            try:
                for task in self.__tasks:
                    self.task_manager.add_new_task(task)
                for task in self.__tasks:
                    try:
                        self.task_manager.get_task_result(task)
                        self.log.debug(
                            "{0} - Items loaded {1}".format(
                                task.thread_name, task.get_total_doc_ops()
                            )
                        )
                    except Exception as e:
                        self.test_log.error(e)
                    finally:
                        self.fail.update(task.fail)
                        self.success.update(task.success)
                        self.fail_count += task.fail_count
                        self.success_count += task.success_count
                        self.resultSeed = task.resultSeed
                        self.log.warning(
                            "Failed to load {0} sub_docs from {1} to {2}".format(
                                task.fail.__len__(),
                                task.generator.start,
                                task.generator.end,
                            )
                        )
            except Exception as e:
                self.test_log.error(e)
                self.set_exception(e)
            finally:
                self.log.debug("========= Tasks in loadgen pool=======")
                self.task_manager.print_tasks_in_pool()
                self.log.debug("======================================")
                for task in self.__tasks:
                    self.task_manager.stop_task(task)
                    self.log.debug(
                        "Task '{0}' complete. Loaded {1} items".format(
                            task.thread_name, task.get_total_doc_ops
                        )
                    )

        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        self.complete_task()
        return self.fail

    def end_task(self):
        self.log.debug("%s - Stopping load" % self.thread_name)
        for task in self.__tasks:
            task.end_task()

    def get_total_doc_ops(self):
        total_ops = 0
        for sub_task in self.__tasks:
            total_ops += sub_task.get_total_doc_ops()
        return total_ops

class DynamoDocLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        username,
        password,
        connection_string,
        provisioned=None,
        read_capacity=None,
        write_capacity=None,
        table=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        fieldsToChange=None,
    ):

        self.thread_name = "Sirius_Dynamo_Load_Task__%s".format(op_type)
        super(DynamoDocLoader, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = "dynamodb"
        self.bucket = None
        self.task_manager = task_manager
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.buckets = None
        self.doc_op_iterations_done = 0
        self.provisioned = provisioned
        self.read_capacity = read_capacity
        self.write_capacity = write_capacity
        self.table = table
        self.identifier_token = IDENTIFIER_TOKEN
        self.username = username
        self.password = password
        self.connection_string = connection_string
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_types = [op_type]
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.fieldsToChange = fieldsToChange
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False

    def call(self):
        try:
            self.start_task()
            if self.op_types:
                if len(self.op_types) != len(self.generators):
                    self.set_exception(Exception("Not all generators have op_type!"))
                    self.complete_task()
            iterator = 0
            for generator in self.generators:
                if self.op_types:
                    self.op_type = self.op_types[iterator]
                self.__tasks.append(
                    RestBasedLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        username=self.username,
                        password=self.password,
                        connection_string=self.connection_string,
                        create_path=self.create_path,
                        sdk_batch_size=self.sdk_batch_size,
                        provisioned=self.provisioned,
                        read_capacity=self.read_capacity,
                        write_capacity=self.write_capacity,
                        table=self.table,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                        fieldsToChange=self.fieldsToChange,
                    )
                )
                iterator += 1
            try:
                for task in self.__tasks:
                    self.task_manager.add_new_task(task)
                for task in self.__tasks:
                    try:
                        self.task_manager.get_task_result(task)
                        self.log.debug(
                            "{0} - Items loaded {1}".format(
                                task.thread_name, task.get_total_doc_ops()
                            )
                        )
                    except Exception as e:
                        self.test_log.error(e)
                    finally:
                        self.fail.update(task.fail)
                        self.success.update(task.success)
                        self.fail_count += task.fail_count
                        self.success_count += task.success_count
                        self.resultSeed = task.resultSeed
                        self.log.warning(
                            "Failed to load {0} sub_docs from {1} to {2}".format(
                                task.fail.__len__(),
                                task.generator.start,
                                task.generator.end,
                            )
                        )
            except Exception as e:
                self.test_log.error(e)
                self.set_exception(e)
            finally:
                self.log.debug("========= Tasks in loadgen pool=======")
                self.task_manager.print_tasks_in_pool()
                self.log.debug("======================================")
                for task in self.__tasks:
                    self.task_manager.stop_task(task)
                    self.log.debug(
                        "Task '{0}' complete. Loaded {1} items".format(
                            task.thread_name, task.get_total_doc_ops
                        )
                    )

        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        self.complete_task()
        return self.fail

    def end_task(self):
        self.log.debug("%s - Stopping load" % self.thread_name)
        for task in self.__tasks:
            task.end_task()

    def get_total_doc_ops(self):
        total_ops = 0
        for sub_task in self.__tasks:
            total_ops += sub_task.get_total_doc_ops()
        return total_ops

class BlobDocLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        aws_access_key=None,
        aws_secret_key=None,
        aws_session_token=None,
        aws_region=None,
        folder_path=None,
        file_format=None,
        file_path=None,
        num_folders=None,
        folders_per_depth=None,
        files_per_folder=None,
        folder_level_names=None,
        max_folder_depth=None,
        min_file_size=None,
        max_file_size=None,
        bucket=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
    ):

        self.thread_name = "Sirius_Blob_Load_Task_%s".format(op_type)
        super(BlobDocLoader, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = "awsS3"
        self.bucket = None
        self.task_manager = task_manager
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.bucket = bucket
        self.doc_op_iterations_done = 0
        self.identifier_token = IDENTIFIER_TOKEN
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_types = [op_type]
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_session_token = aws_session_token
        self.aws_region = aws_region
        self.folder_path = folder_path
        self.file_format = file_format
        self.file_path = file_path
        self.folders_per_depth = folders_per_depth
        self.files_per_folder = files_per_folder
        self.folder_level_names = folder_level_names
        self.max_folder_depth = max_folder_depth
        self.min_file_size = min_file_size
        self.max_file_size = max_file_size
        self.num_folders = num_folders
    def call(self):
        try:
            self.start_task()
            if self.op_types:
                if len(self.op_types) != len(self.generators):
                    self.set_exception(Exception("Not all generators have op_type!"))
                    self.complete_task()
            iterator = 0
            for generator in self.generators:
                if self.op_types:
                    self.op_type = self.op_types[iterator]
                self.__tasks.append(
                    RestBasedLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        bucket=self.bucket,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                        fieldsToChange=self.fieldsToChange,
                        aws_access_key = self.aws_access_key,
                        aws_secret_key = self.aws_secret_key,
                        aws_session_token = self.aws_session_token,
                        aws_region = self.aws_region,
                        folder_path = self.folder_path,
                        file_format = self.file_format,
                        file_path = self.file_path,
                        folders_per_depth = self.folders_per_depth,
                        files_per_folder = self.files_per_folder,
                        folder_level_names = self.folder_level_names,
                        max_folder_depth = self.max_folder_depth,
                        min_file_size = self.min_file_size,
                        max_file_size = self.max_file_size,
                        num_folders = self.num_folders,
                    )
                )
                iterator += 1
            try:
                for task in self.__tasks:
                    self.task_manager.add_new_task(task)
                for task in self.__tasks:
                    try:
                        self.task_manager.get_task_result(task)
                        self.log.debug(
                            "{0} - Items loaded {1}".format(
                                task.thread_name, task.get_total_doc_ops()
                            )
                        )
                    except Exception as e:
                        self.test_log.error(e)
                    finally:
                        self.fail.update(task.fail)
                        self.success.update(task.success)
                        self.fail_count += task.fail_count
                        self.success_count += task.success_count
                        self.resultSeed = task.resultSeed
                        self.log.warning(
                            "Failed to load {0} sub_docs from {1} to {2}".format(
                                task.fail.__len__(),
                                task.generator.start,
                                task.generator.end,
                            )
                        )
            except Exception as e:
                self.test_log.error(e)
                self.set_exception(e)
            finally:
                self.log.debug("========= Tasks in loadgen pool=======")
                self.task_manager.print_tasks_in_pool()
                self.log.debug("======================================")
                for task in self.__tasks:
                    self.task_manager.stop_task(task)
                    self.log.debug(
                        "Task '{0}' complete. Loaded {1} items".format(
                            task.thread_name, task.get_total_doc_ops
                        )
                    )

        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        self.complete_task()
        return self.fail

    def end_task(self):
        self.log.debug("%s - Stopping load" % self.thread_name)
        for task in self.__tasks:
            task.end_task()

    def get_total_doc_ops(self):
        total_ops = 0
        for sub_task in self.__tasks:
            total_ops += sub_task.get_total_doc_ops()
        return total_ops

class ValidateColumnarDocs(Task):
    def __init__(
        self,
        task_manager,
        generator,
        db_type,
        username,
        password,
        connection_string,
        bucket=None,
        scope=None,
        collection=None,
        database=None,
        keyspace=None,
        table=None,
        columnar_conn_str=None,
        columnar_username=None,
        columnar_password=None,
        columnar_bucket=None,
        columnar_scope=None,
        columnar_collection=None,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
    ):

        self.thread_name = "Sirius_Validate_Columnar_Task_%s".format( db_type )
        super(ValidateColumnarDocs, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = db_type
        self.bucket = None
        self.task_manager = task_manager
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.bucket = bucket
        self.op_type = "validateColumnar"
        self.doc_op_iterations_done = 0
        self.identifier_token = IDENTIFIER_TOKEN
        if isinstance(self.op_type, list):
            self.op_types = self.op_type
        else:
            self.op_types = [self.op_type]
        self.num_loaded = 0
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False
        self.columnar_conn_str = columnar_conn_str
        self.columnar_username = columnar_username
        self.columnar_password = columnar_password
        self.columnar_bucket = columnar_bucket
        self.columnar_scope = columnar_scope
        self.columnar_collection = columnar_collection
        self.username = username
        self.password = password
        self.connection_string = connection_string
        self.database = database
        self.keyspace = keyspace
        self.table = table
        self.scope = scope
        self.collection = collection

    def call(self):
        try:
            self.start_task()
            if self.op_types:
                if len(self.op_types) != len(self.generators):
                    self.set_exception(Exception("Not all generators have op_type!"))
                    self.complete_task()
            iterator = 0
            for generator in self.generators:
                if self.op_types:
                    self.op_type = self.op_types[iterator]
                self.__tasks.append(
                    RestBasedLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        username=self.username,
                        password=self.password,
                        connection_string=self.connection_string,
                        bucket=self.bucket,
                        scope=self.scope,
                        collection=self.collection,
                        database=self.database,
                        keyspace=self.keyspace,
                        table=self.table,
                        columnar_conn_str=self.columnar_conn_str,
                        columnar_username=self.columnar_username,
                        columnar_password=self.columnar_password,
                        columnar_bucket=self.columnar_bucket,
                        columnar_scope=self.columnar_scope,
                        columnar_collection=self.columnar_collection,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                    )
                )
                iterator += 1
            try:
                for task in self.__tasks:
                    self.task_manager.add_new_task(task)
                for task in self.__tasks:
                    try:
                        self.task_manager.get_task_result(task)
                        self.log.debug(
                            "{0} - Items loaded {1}".format(
                                task.thread_name, task.get_total_doc_ops()
                            )
                        )
                    except Exception as e:
                        self.test_log.error(e)
                    finally:
                        self.fail.update(task.fail)
                        self.success.update(task.success)
                        self.fail_count += task.fail_count
                        self.success_count += task.success_count
                        self.resultSeed = task.resultSeed
                        self.log.warning(
                            "Failed to load {0} sub_docs from {1} to {2}".format(
                                task.fail.__len__(),
                                task.generator.start,
                                task.generator.end,
                            )
                        )
            except Exception as e:
                self.test_log.error(e)
                self.set_exception(e)
            finally:
                self.log.debug("========= Tasks in loadgen pool=======")
                self.task_manager.print_tasks_in_pool()
                self.log.debug("======================================")
                for task in self.__tasks:
                    self.task_manager.stop_task(task)
                    self.log.debug(
                        "Task '{0}' complete. Loaded {1} items".format(
                            task.thread_name, task.get_total_doc_ops
                        )
                    )

        except Exception as e:
            self.test_log.error(e)
            self.set_exception(e)
        self.complete_task()
        return self.fail

    def end_task(self):
        self.log.debug("%s - Stopping load" % self.thread_name)
        for task in self.__tasks:
            task.end_task()

    def get_total_doc_ops(self):
        total_ops = 0
        for sub_task in self.__tasks:
            total_ops += sub_task.get_total_doc_ops()
        return total_ops