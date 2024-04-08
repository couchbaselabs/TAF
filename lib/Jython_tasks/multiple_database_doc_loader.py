import traceback

from constants.cloud_constants import DocLoading
from Jython_tasks.task import Task
from doc_loader.sirius_client import RESTClient
from pytests.sdk_workloads.go_workload import IDENTIFIER_TOKEN


class RestBasedCloudDocLoader(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        db_type,
        username,
        password,
        connection_string,
        exp=None,
        exp_unit="seconds",
        random_exp=False,
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
        flag=0,
        time_unit="seconds",
        timeout_secs=5,
        compression=None,
        retries=5,
        task_identifier="",
        skip_read_on_error=False,
        suppress_error_table=False,
        sdk_retry_strategy=None,
        iterations=1,
        track_failures=True,
        scheme="http",
        read_your_own_write=False,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        retry=1000,
        retry_interval=0.2,
        delete_record=False,
        ignore_exceptions=[],
        retry_exception=[],
        retry_attempts=0,
        fieldsToChange=None,
        cluster=None,
    ):
        self.thread_name = "Sirius_Based_Load_Task_%s_%s".format(db_type, op_type)
        super(RestBasedCloudDocLoader, self).__init__(self.thread_name)
        self.task_manager = task_manager
        self.generator = generator
        self.op_type = op_type
        self.db_type = db_type
        self.exp = exp
        self.cluster = cluster
        self.exp_unit = exp_unit
        self.random_exp = random_exp
        self.flag = flag
        self.time_unit = time_unit
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.compression_disabled = compression_disabled
        self.compression_min_size = compression_min_size
        self.compression_min_ratio = compression_min_ratio
        self.connection_timeout = connection_timeout
        self.retries = retries
        self.kv_timeout = kv_timeout
        self.kv_durable_timeout = kv_durable_timeout
        self.bucket = bucket
        self.scope = scope
        self.collection = collection
        self.expiry = expiry
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.durability = durability
        self.task_identifier = task_identifier
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
        self.skip_read_on_error = skip_read_on_error
        self.suppress_error_table = suppress_error_table
        self.sdk_retry_strategy = sdk_retry_strategy
        self.req_iterations = iterations
        self.doc_op_iterations_done = 0
        self.num_loaded = 0
        self.track_failures = track_failures
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.identifier_token = IDENTIFIER_TOKEN
        self.scheme = scheme
        if db_type == "couchbase":
            self.username = self.cluster.username
            self.password = self.cluster.password
        else:
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
        self.expiry = self.exp
        self.readYourOwnWrite = read_your_own_write
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.ignore_exceptions = ignore_exceptions
        self.retry_exceptions = retry_exception
        self.retry_attempts = retry_attempts
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False
        self.fieldsToChange = fieldsToChange

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
                        retry=self.retry,
                        retry_interval=self.retry_interval,
                        delete_record=self.delete_record,
                    )
                )

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
                        retry=self.retry,
                        retry_interval=self.retry_interval,
                        delete_record=self.delete_record,
                    )
                )

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

class RestBasedCloudDocLoaderAbstract(Task):
    def __init__(
        self,
        task_manager,
        generator,
        op_type,
        db_type,
        username,
        password,
        connection_string,
        exp=None,
        exp_unit="seconds",
        random_exp=False,
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
        flag=0,
        time_unit="seconds",
        timeout_secs=5,
        compression=None,
        retries=5,
        task_identifier="",
        skip_read_on_error=False,
        suppress_error_table=False,
        sdk_retry_strategy=None,
        iterations=1,
        track_failures=True,
        scheme="http",
        read_your_own_write=False,
        template="Person",
        sirius_base_url="http://0.0.0.0:4000",
        retry=1000,
        retry_interval=0.2,
        delete_record=False,
        ignore_exceptions=[],
        retry_exception=[],
        retry_attempts=0,
        fieldsToChange=None,
        batch_size=1,
        process_concurrency=8,
        print_ops_rate=True,
        monitor_stats=("doc_ops",),
        cluster=None,
    ):

        self.thread_name = "Sirius_Based_Load_task_Abstract_%s_%s".format(
            db_type, op_type
        )
        super(RestBasedCloudDocLoaderAbstract, self).__init__(self.thread_name)
        self.op_type = None
        self.db_type = db_type
        self.bucket = None
        self.cluster = cluster
        self.exp = exp
        self.random_exp = random_exp
        self.exp_unit = exp_unit
        self.flag = flag
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.time_unit = time_unit
        self.timeout_secs = timeout_secs
        self.compression = compression
        self.process_concurrency = process_concurrency
        self.task_manager = task_manager
        self.batch_size = batch_size
        self.generators = generator
        self.input_generators = generator
        self.op_types = None
        self.buckets = None
        self.print_ops_rate = print_ops_rate
        self.retries = retries
        self.durability = durability
        self.task_identifier = task_identifier
        self.skip_read_on_error = skip_read_on_error
        self.monitor_stats = monitor_stats
        self.scope = scope
        self.collection = collection
        self.preserve_expiry = preserve_expiry
        self.sdk_retry_strategy = sdk_retry_strategy
        self.req_iterations = iterations
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
        self.suppress_error_table = suppress_error_table
        self.sdk_retry_strategy = sdk_retry_strategy
        self.identifier_token = IDENTIFIER_TOKEN
        self.scheme = scheme
        if db_type == "couchbase":
            self.username = self.cluster.username
            self.password = self.cluster.password
        else:
            self.username = username
            self.password = password
        self.connection_string = connection_string
        self.columnar_username = columnar_username
        self.columnar_password = columnar_password
        self.columnar_conn_str = columnar_conn_str
        if isinstance(op_type, list):
            self.op_types = op_type
        else:
            self.op_types = [op_type]
        if db_type == "couchbase":
            if isinstance(bucket, list):
                self.buckets = bucket
            else:
                self.buckets = [bucket]
        self.num_loaded = 0
        self.track_failures = track_failures
        self.fail = dict()
        self.success = dict()
        self.print_ops_rate_tasks = list()
        self.__tasks = list()
        self.expiry = exp
        self.read_your_own_write = read_your_own_write
        self.fieldsToChange = fieldsToChange
        self.template = template
        self.sirius_base_url = sirius_base_url
        self.retry = retry
        self.retry_interval = retry_interval
        self.delete_record = delete_record
        self.ignore_exceptions = ignore_exceptions
        self.retry_exceptions = retry_exception
        self.retry_attempts = retry_attempts
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
                    RestBasedCloudDocLoader(
                        task_manager=self.task_manager,
                        generator=generator,
                        op_type=self.op_type,
                        db_type=self.db_type,
                        username=self.username,
                        password=self.password,
                        connection_string=self.connection_string,
                        exp=self.exp,
                        exp_unit=self.exp_unit,
                        random_exp=self.random_exp,
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
                        database=self.database,
                        query=self.query,
                        columnar_conn_str=self.columnar_conn_str,
                        columnar_username=self.columnar_username,
                        columnar_password=self.columnar_password,
                        columnar_bucket=self.columnar_bucket,
                        columnar_scope=self.columnar_scope,
                        columnar_collection=self.columnar_collection,
                        provisioned=self.provisioned,
                        read_capacity=self.read_capacity,
                        write_capacity=self.write_capacity,
                        keyspace=self.keyspace,
                        table=self.table,
                        num_of_conns=self.num_of_conns,
                        sub_doc_path=self.sub_doc_path,
                        replication_factor=self.replication_factor,
                        cassandra_class=self.cassandra_class,
                        port=self.port,
                        max_idle_connections=self.max_idle_connections,
                        max_open_connections=self.max_open_connections,
                        max_idle_time=self.max_idle_time,
                        max_life_time=self.max_life_time,
                        flag=self.flag,
                        time_unit="seconds",
                        timeout_secs=self.timeout_secs,
                        compression=self.compression,
                        retries=5,
                        task_identifier=self.task_identifier,
                        skip_read_on_error=self.skip_read_on_error,
                        suppress_error_table=self.suppress_error_table,
                        sdk_retry_strategy=self.sdk_retry_strategy,
                        iterations=1,
                        track_failures=self.track_failures,
                        scheme=self.scheme,
                        read_your_own_write=self.read_your_own_write,
                        template=self.template,
                        sirius_base_url=self.sirius_base_url,
                        retry=self.retry,
                        retry_interval=self.retry_interval,
                        delete_record=self.delete_record,
                        ignore_exceptions=self.ignore_exceptions,
                        retry_exception=self.retry_exceptions,
                        retry_attempts=self.retry_attempts,
                        fieldsToChange=self.fieldsToChange,
                        cluster=self.cluster,
                    )
                )
                iterator += 1
            if self.print_ops_rate and self.db_type == "couchbase":
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
                if self.print_ops_rate and self.db_type == "couchbase":
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
