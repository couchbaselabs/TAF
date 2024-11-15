from Jython_tasks.task_manager import Task
from cb_constants import DocLoading
from common_lib import IDENTIFIER_TOKEN
from sirius_client_framework.multiple_database_config import CouchbaseLoader, MongoLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from sirius_client_framework.sirius_client import SiriusClient
from sirius_client_framework.sirius_constants import SiriusCodes
from sirius_client_framework.sirius_setup import SiriusSetup


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
                 ignore_exceptions=[], retry_exception=[], iterations=1,
                 retry_exceptions_task=False):
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

        self.retry_exceptions_task = retry_exceptions_task

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
        elif self.op_type == SiriusCodes.DocOps.RETRY_EXCEPTIONS:
            op_type = SiriusCodes.DocOps.RETRY_EXCEPTIONS
        return WorkLoadTask(
            bucket=self.bucket,
            task_manager=self.task_manager, op_type=op_type,
            database_information=db_info,
            operation_config=operation_config,
            default_sirius_base_url=SiriusSetup.sirius_url)


class WorkLoadTask(Task):
    def __init__(self, task_manager, op_type, database_information,
                 operation_config,
                 default_sirius_base_url="http://0.0.0.0:4000", bucket=None):
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
        self.thread_name = (f"WorkLoadTask{database_information.db_type}_"
                            f"{op_type}")
        super(WorkLoadTask, self).__init__(self.thread_name)
        self.bucket = bucket
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
            self.set_result(True)
        except Exception as e:
            self.log.critical(e)
            self.set_result(False)
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
            self.set_result(True)

        except Exception as e:
            self.log.critical(e)
            self.set_result(False)

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

class MongoUtil(object):

    def __init__(self, task_manager, hostname, username, password):
        connection_str = (f"mongodb://{username}:{password}@"
                          f"{hostname}/?retryWrites=true&w=majority&replicaSet=rs0")
        self.loader = MongoLoader(username=username, password=password,
                                  connection_string=connection_str)
        self.task_manager = task_manager

        """
        use this operation config for following use cases -
        1. create/delete mongo databases.
        2. create/delete mongo collections.
        """
        self.no_op_operation_config = WorkloadOperationConfig(
            start=0, end=0, template="no_op_config", doc_size=1024)

    """
    Method creates a database in Mongo, the created database will not show 
    up on UI as mongo does not display databases which does not have any 
    collections.
    Note - Use this method only if you want to create empty mongo database, 
    otherwise use create_mongo_collection method for all other use-cases.
    """
    def create_mongo_database(self, database):
        self.loader.database = database
        self.loader.collection = None
        task_create = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.CREATE,
            database_information=self.loader,
            operation_config=self.no_op_operation_config)
        self.task_manager.add_new_task(task_create)
        self.task_manager.get_task_result(task_create)
        return task_create.result

    """
    Method deletes a specified database. This method will delete all the 
    collections within the database.
    """
    def delete_mongo_database(self, database):
        self.loader.database = database
        self.loader.collection = None
        task_create = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.DELETE,
            database_information=self.loader,
            operation_config=self.no_op_operation_config)
        self.task_manager.add_new_task(task_create)
        self.task_manager.get_task_result(task_create)
        return task_create.result

    """
    Method creates a database and collection in Mongo. If database is 
    already present then it will only create collection under that database.
    """
    def create_mongo_collection(self, database, collection):
        self.loader.database = database
        self.loader.collection = collection
        task_create = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.CREATE,
            database_information=self.loader,
            operation_config=self.no_op_operation_config
        )
        self.task_manager.add_new_task(task_create)
        self.task_manager.get_task_result(task_create)
        return task_create.result

    """
    Method deletes a specified collection within the specified database. If 
    the collection being deleted is the last collection in the database then 
    the database is deleted as well. 
    """
    def delete_mongo_collection(self, database, collection):
        self.loader.database = database
        self.loader.collection = collection
        task_create = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.DELETE,
            database_information=self.loader,
            operation_config=self.no_op_operation_config)
        self.task_manager.add_new_task(task_create)
        self.task_manager.get_task_result(task_create)
        return task_create.result

    """
    Method loads docs in the specified mongoDB collection.
    """
    def load_docs_in_mongo_collection(
            self, database, collection, start, end,
            doc_template=SiriusCodes.Templates.PERSON, doc_size=1024,
            sdk_batch_size=500, wait_for_task_complete=False):
        self.loader.database = database
        self.loader.collection = collection
        self.loader.sdk_batch_size = sdk_batch_size

        operation_config = WorkloadOperationConfig(
            start=start, end=end, template=doc_template, doc_size=doc_size)
        task = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_CREATE,
            database_information=self.loader,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task)
        if wait_for_task_complete:
            self.task_manager.get_task_result(task)
        return task

    """
    Method deletes docs from the specified mongoDB collection.
    """
    def delete_docs_from_mongo_collection(
            self, database, collection, start, end, sdk_batch_size=500,
            wait_for_task_complete=False):
        self.loader.database = database
        self.loader.collection = collection
        self.loader.sdk_batch_size = sdk_batch_size

        operation_config = WorkloadOperationConfig(
            start=start, end=end, template="", doc_size=1024)
        task = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_DELETE,
            database_information=self.loader,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task)
        if wait_for_task_complete:
            self.task_manager.get_task_result(task)
        return task

    """
    Method updates docs in the specified mongoDB collection.
    """
    def update_docs_in_mongo_collection(
            self, database, collection, start, end,
            fields_to_update=[], doc_template=SiriusCodes.Templates.PERSON,
            doc_size=1024, sdk_batch_size=500, wait_for_task_complete=False):
        self.loader.database = database
        self.loader.collection = collection
        self.loader.sdk_batch_size = sdk_batch_size

        if not fields_to_update:
            fields_to_update = None
        operation_config = WorkloadOperationConfig(
            start=start, end=end, template=doc_template, doc_size=doc_size,
            fields_to_change=fields_to_update)
        task = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_UPDATE,
            database_information=self.loader,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task)
        if wait_for_task_complete:
            self.task_manager.get_task_result(task)
        return task

    """
    Method gets the doc count in the specified mongoDB collection.
    """
    def get_collection_doc_count(self, database, collection):
        self.loader.database = database
        self.loader.collection = collection
        task = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.COUNT,
            database_information=self.loader,
            operation_config=self.no_op_operation_config)
        self.task_manager.add_new_task(task)
        self.task_manager.get_task_result(task)
        if task.result and not task.error:
            return task.data
        else:
            raise Exception(task.error)

    """
    Method performs creation, updation and deletion of docs in the 
    specified mongoDB collection.
    """
    def perform_crud_op_on_mongo_collection(
            self, database, collection, start, end,
            percentage_create=100, percentage_update=0, percentage_delete=0,
            fields_to_update=[], doc_template=SiriusCodes.Templates.PERSON,
            doc_size=1024, sdk_batch_size=500, wait_for_task_complete=False):

        if (percentage_create + percentage_update + percentage_delete) > 100:
            raise Exception(
                "Total value of percentage_create + percentage_update + "
                "percentage_delete cannot be greater than 100")

        doc_tasks = list()

        if percentage_delete:
            delete_end_counter = int(((end - start) * percentage_delete) //
                                     100)
            doc_tasks.append(self.delete_docs_from_mongo_collection(
                database, collection, start, delete_end_counter,
                sdk_batch_size, False))
            start = delete_end_counter

        if percentage_create:
            create_end = int(((end - start) * percentage_create) // 100) + end
            doc_tasks.append(self.load_docs_in_mongo_collection(
                database, collection, end, create_end, doc_template, doc_size,
                sdk_batch_size, False))
            end = create_end

        if percentage_update:
            update_end = (int(((end - start) * percentage_update) // 100) +
                          start)
            doc_tasks.append(self.update_docs_in_mongo_collection(
                database, collection, start, update_end, fields_to_update,
                doc_template, doc_size, sdk_batch_size))

        if wait_for_task_complete:
            for task in doc_tasks:
                self.task_manager.get_task_result(task)
            return [], start, end
        else:
            return doc_tasks, start, end


class CouchbaseUtil(object):

    def __init__(self, task_manager, hostname, username, password):
        self.loader = CouchbaseLoader(
            username=username, password=password,
            connection_string=f"couchbases://{hostname}")
        self.task_manager = task_manager

    """
    Method loads docs in the specified couchbase collection.
    """
    def load_docs_in_couchbase_collection(
            self, bucket, scope, collection, start, end,
            doc_template=SiriusCodes.Templates.PERSON, doc_size=1024,
            sdk_batch_size=500):
        self.loader.bucket = bucket
        self.loader.scope = scope
        self.loader.collection = collection
        self.loader.sdk_batch_size = sdk_batch_size

        operation_config = WorkloadOperationConfig(
            start=start, end=end, template=doc_template, doc_size=doc_size)
        task = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_CREATE,
            database_information=self.loader,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task)
        self.task_manager.get_task_result(task)
        return task

    """
    Method deletes docs from the specified couchbase collection.
    """
    def delete_docs_from_couchbase_collection(
            self, bucket, scope, collection, start, end, sdk_batch_size=500):
        self.loader.bucket = bucket
        self.loader.scope = scope
        self.loader.collection = collection
        self.loader.sdk_batch_size = sdk_batch_size

        operation_config = WorkloadOperationConfig(
            start=start, end=end, template="", doc_size=1024)
        task = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_DELETE,
            database_information=self.loader,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task)
        self.task_manager.get_task_result(task)
        return task

    """
    Method updates docs in the specified couchbase collection.
    """
    def update_docs_in_couchbase_collection(
            self, bucket, scope, collection, start, end,
            fields_to_update=[], doc_template=SiriusCodes.Templates.PERSON,
            doc_size=1024, sdk_batch_size=500):
        self.loader.bucket = bucket
        self.loader.scope = scope
        self.loader.collection = collection
        self.loader.sdk_batch_size = sdk_batch_size

        if not fields_to_update:
            fields_to_update = None
        operation_config = WorkloadOperationConfig(
            start=start, end=end, template=doc_template, doc_size=doc_size,
            fields_to_change=fields_to_update)
        task = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_UPDATE,
            database_information=self.loader,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task)
        self.task_manager.get_task_result(task)
        return task

    """
    Method performs creation, updation and deletion of docs in the 
    specified couchbase collection.
    """
    def perform_crud_op_on_couchbase_collection(
            self, bucket, scope, collection, start, end,
            percentage_create=100, percentage_update=0, percentage_delete=0,
            fields_to_update=[], doc_template=SiriusCodes.Templates.PERSON,
            doc_size=1024, sdk_batch_size=500):

        if (percentage_create + percentage_update + percentage_delete) > 100:
            raise Exception(
                "Total value of percentage_create + percentage_update + "
                "percentage_delete cannot be greater than 100")

        if percentage_delete:
            delete_end_counter = int(end - start) * int(
                percentage_delete / 100)
            self.delete_docs_from_couchbase_collection(
                bucket, scope, collection, start, delete_end_counter,
                sdk_batch_size)
            start = delete_end_counter

        if percentage_create:
            create_end = int(end - start) * int(percentage_create / 100)
            self.load_docs_in_couchbase_collection(
                bucket, scope, collection, end, create_end, doc_template,
                doc_size, sdk_batch_size)
            end = create_end

        if percentage_update:
            update_end = int(end - start) * int(percentage_update / 100)
            self.update_docs_in_couchbase_collection(
                bucket, scope, collection, start, update_end, fields_to_update,
                doc_template, doc_size, sdk_batch_size)

        return "status"
