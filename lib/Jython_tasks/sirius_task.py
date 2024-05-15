from cb_constants import DocLoading
from common_lib import IDENTIFIER_TOKEN
from sirius_client_framework.multiple_database_config import CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from sirius_client_framework.sirius_client import SiriusClient
from sirius_client_framework.sirius_constants import SiriusCodes
from sirius_client_framework.sirius_setup import SiriusSetup
from Jython_tasks.task import Task


class LoadCouchbaseDocs(object):
    def __init__(self, task_manager, cluster, use_https,
                 bucket, scope, collection, generator, op_type,
                 exp=0, persist_to=0, replicate_to=0, durability="",
                 timeout_secs=5, batch_size=100, process_concurrency=1,
                 active_resident_threshold=100, print_ops_rate=False,
                 task_identifier="", dgm_batch=100, track_failures=True,
                 preserve_expiry=False, sdk_retry_strategy=None,
                 create_paths=False, xattr=False,
                 store_semantics=None, access_deleted=False,
                 create_as_deleted=False,
                 ignore_exceptions=[], retry_exception=[], iterations=1):
        self.task_manager = task_manager
        # Cluster specific params
        self.cluster = cluster
        self.use_https = use_https

        # Bucket / collection specific params
        self.bucket = bucket
        self.scope = scope
        self.collection = collection

        # SDK client operation specific params
        self.persist_to = persist_to
        self.replicate_to = replicate_to
        self.durability = durability
        self.exp = exp
        self.timeout_secs = timeout_secs
        self.preserve_expiry = preserve_expiry
        self.sdk_retry_strategy = sdk_retry_strategy
        # Sub-doc operations specific SDK params
        self.create_paths = create_paths
        self.xattr = xattr
        self.store_semantics = store_semantics
        self.access_deleted = access_deleted
        self.create_as_deleted = create_as_deleted

        # Load specific attributes
        self.generator = generator
        self.op_type = op_type
        self.batch_size = batch_size
        self.process_concurrency = process_concurrency
        self.active_resident_threshold = active_resident_threshold
        self.print_ops_rate = print_ops_rate
        self.task_identifier = task_identifier
        self.dgm_batch = dgm_batch
        self.track_failures = track_failures
        self.iterations = iterations
        self.ignore_exceptions = ignore_exceptions
        self.retry_exception = retry_exception

    def create_sirius_task(self):
        if self.use_https:
            conn_str = f"couchbases://{self.cluster.master.ip}"
        else:
            conn_str = f"couchbase://{self.cluster.master.ip}"
        db_info = CouchbaseLoader(username=self.cluster.username,
                                  password=self.cluster.password,
                                  connection_string=conn_str,
                                  bucket=self.bucket.name,
                                  scope=self.scope,
                                  collection=self.collection,
                                  sdk_batch_size=self.batch_size,
                                  # compression_disabled=None,
                                  # compression_min_size=None,
                                  # compression_min_ratio=None,
                                  expiry=self.exp,
                                  persist_to=self.persist_to,
                                  replicate_to=self.replicate_to,
                                  durability=self.durability,
                                  operation_timeout=self.timeout_secs,
                                  preserve_expiry=self.preserve_expiry,
                                  create_path=self.create_paths,
                                  is_xattr=self.xattr,
                                  store_semantic=self.store_semantics,
                                  # access_deleted=self.access_deleted,
                                  # create_as_deleted=self.create_as_deleted,
                                  )
        operation_config = WorkloadOperationConfig(
            start=self.generator.start,
            end=self.generator.end,
            template=SiriusCodes.Templates.PERSON,
            doc_size=self.generator.doc_size)
        op_type = None
        if self.op_type == DocLoading.Bucket.DocOps.CREATE:
            op_type = SiriusCodes.DocOps.CREATE
        elif self.op_type == DocLoading.Bucket.DocOps.UPDATE:
            op_type = SiriusCodes.DocOps.UPDATE
        elif self.op_type == DocLoading.Bucket.DocOps.READ:
            op_type = SiriusCodes.DocOps.READ
        elif self.op_type == DocLoading.Bucket.DocOps.TOUCH:
            op_type = SiriusCodes.DocOps.TOUCH
        elif self.op_type == DocLoading.Bucket.DocOps.REPLACE:
            op_type = SiriusCodes.DocOps.REPLACE
        elif self.op_type == DocLoading.Bucket.DocOps.DELETE:
            op_type = SiriusCodes.DocOps.DELETE
        elif self.op_type == SiriusCodes.DocOps.VALIDATE:
            op_type = SiriusCodes.DocOps.VALIDATE
        return WorkLoadTask(
            task_manager=self.task_manager, op_type=op_type,
            database_information=db_info,
            operation_config=operation_config,
            default_sirius_base_url=SiriusSetup.sirius_url)


class WorkLoadTask(Task):
    def __init__(self, task_manager, op_type, database_information,
                 operation_config,
                 default_sirius_base_url="http://0.0.0.0:4000"):
        """
        :param task_manager: Instance of TaskManager class
            (Jython_tasks.task_manager.TaskManager)
        :param op_type: Instance of SiriusCodes class
            (sirius_client_framework.sirius_constants.SiriusCodes)
        :param database_information: Instance of DB class
            (sirius_client_framework.multiple_database_config.DB)
        :param operation_config: Instance of OperationConfig class
            (sirius_client_framework.operation_config.OperationConfig)
        :param default_sirius_base_url: Sirius Server URL with port (string)
        """
        self.thread_name = "WorkLoadTask%s_%s" \
            .format(database_information.db_type, op_type)
        super(WorkLoadTask, self).__init__(self.thread_name)
        self.op_type = op_type
        self.sirius_base_url = default_sirius_base_url
        self.database_information = database_information
        self.operation_config = operation_config
        self.task_manager = task_manager
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = None
        self.result = False
        self.fail = None

    def call(self):
        self.start_task()
        try:
            sirius_client_object = SiriusClient(
                self.sirius_base_url,
                identifier_token=IDENTIFIER_TOKEN)
            self.fail, self.success_count, self.fail_count, self.resultSeed \
                = sirius_client_object.start_workload(
                    self.op_type,
                    self.database_information,
                    self.operation_config)
            self.test_log.debug("success_count : " + str(self.success_count))
            self.test_log.debug("fail_count : " + str(self.fail_count))
            self.test_log.debug("failed_items: " + "".join(self.fail.items()))
        except Exception as e:
            self.log.critical(e)
        self.set_result(True)
        self.complete_task()

    def get_total_doc_ops(self):
        return self.operation_config.end - self.operation_config.start


class DatabaseManagementTask(Task):
    def __init__(self, task_manager, op_type, database_information,
                 operation_config,
                 default_sirius_base_url="http://0.0.0.0:4000"):
        """
        :param task_manager: Instance of TaskManager class
            (Jython_tasks.task_manager.TaskManager)
        :param op_type: Instance of SiriusCodes class
            (sirius_client_framework.sirius_constants.SiriusCodes)
        :param database_information: Instance of DB class
            (sirius_client_framework.multiple_database_config.DB)
        :param operation_config: Instance of OperationConfig class
            (sirius_client_framework.operation_config.OperationConfig)
        :param default_sirius_base_url: Sirius Server URL with port (string)
        """
        self.thread_name = "DatabaseManagementTask%s_%s"\
            .format(database_information.db_type, op_type)
        super(DatabaseManagementTask, self).__init__(self.thread_name)
        self.op_type = op_type
        self.sirius_base_url = default_sirius_base_url
        self.database_information = database_information
        self.operation_config = operation_config
        self.task_manager = task_manager
        self.error = ""
        self.message = ""
        self.data = ""

    def call(self):
        self.start_task()
        try:
            sirius_client_object = SiriusClient(
                self.sirius_base_url,
                identifier_token=IDENTIFIER_TOKEN)
            self.error, self.message, self.data \
                = sirius_client_object.database_management(
                    op_type=self.op_type,
                    database_info=self.database_information,
                    operation_config=self.operation_config)
            self.test_log.debug(self.message)

        except Exception as e:
            self.log.critical(e)
        self.set_result(True)
        self.complete_task()

    def get_total_doc_ops(self):
        return self.operation_config.end - self.operation_config.start


class BlobLoadTask(Task):
    def __init__(self, task_manager, op_type, operation_config,
                 external_storage_config,
                 default_sirius_base_url="http://0.0.0.0:4000"):
        """
        :param task_manager: Instance of TaskManager class
            (Jython_tasks.task_manager.TaskManager)
        :param op_type: Instance of SiriusCodes class
            (sirius_client_framework.sirius_constants.SiriusCodes)
        :param external_storage_config: Instance of ExternalStorage class
            (sirius_client_framework.external_storage_config.ExternalStorage)
        :param operation_config: Instance of OperationConfig
            (sirius_client_framework.operation_config.OperationConfig)
        :param default_sirius_base_url: Sirius Server URL with port (string)
        """
        self.thread_name = "BlobLoadTask%s_%s"\
            .format(external_storage_config.cloud_provider, op_type)
        super(BlobLoadTask, self).__init__(self.thread_name)
        self.op_type = op_type
        self.sirius_base_url = default_sirius_base_url
        self.operation_config = operation_config
        self.external_storage_config = external_storage_config
        self.task_manager = task_manager
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = None
        self.result = False
        self.fail = None

    def call(self):
        self.start_task()
        try:
            sirius_client_object = SiriusClient(
                self.sirius_base_url,
                identifier_token=IDENTIFIER_TOKEN)
            self.fail, self.success_count, self.fail_count, self.resultSeed \
                = sirius_client_object.start_blob_workload(
                    op_type=self.op_type,
                    operation_config=self.operation_config,
                    external_storage_config=self.external_storage_config)
            self.test_log.debug("success_count : " + str(self.success_count))
            self.test_log.debug("fail_count : " + str(self.fail_count))
            self.test_log.debug("failed_items: " + "".join(self.fail.items()))

        except Exception as e:
            self.log.critical(e)
        self.set_result(True)
        self.complete_task()

    def get_total_doc_ops(self):
        return self.operation_config.end - self.operation_config.start
