from task import Task
from sirius_client_framework.sirius_client import SiriusClient


class WorkLoadTask(Task):
    def __init__(self, task_manager, op_type, database_information, operation_config,
                 default_sirius_base_url="http://0.0.0.0:4000"):
        """
            :param task_manager: Instance of TaskManager class (Jython_tasks.task_manager.TaskManager)
            :param op_type: Instance of SiriusCodes class (sirius_client_framework.sirius_constants.SiriusCodes)
            :param database_information: Instance of DB class (sirius_client_framework.multiple_database_config.DB)
            :param operation_config: Instance of OperationConfig class (sirius_client_framework.operation_config.OperationConfig)
            :param default_sirius_base_url: Url with port specification of Sirius Server (string)
        """
        self.thread_name = "WorkLoadTask%s_%s".format(database_information.db_type, op_type)
        super(WorkLoadTask, self).__init__(self.thread_name)
        self.op_type = op_type
        self.sirius_base_url = default_sirius_base_url
        self.database_information = database_information
        self.operation_config = operation_config
        self.task_manager = task_manager
        self.fail = dict()
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False

    def call(self):
        try:
            self.start_task()
            sirius_client_object = SiriusClient()
            self.fail, self.success_count, self.fail_count, self.resultSeed = sirius_client_object.start_workload(
                op_type=self.op_type,
                database_info=self.database_information,
                operation_config=self.operation_config,
            )
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
    def __init__(self, task_manager, op_type, database_information, operation_config,
                 default_sirius_base_url="http://0.0.0.0:4000"):
        """
            :param task_manager: Instance of TaskManager class (Jython_tasks.task_manager.TaskManager)
            :param op_type: Instance of SiriusCodes class (sirius_client_framework.sirius_constants.SiriusCodes)
            :param database_information: Instance of DB class (sirius_client_framework.multiple_database_config.DB)
            :param operation_config: Instance of OperationConfig class (sirius_client_framework.operation_config.OperationConfig)
            :param default_sirius_base_url: Url with port specification of Sirius Server (string)
        """
        self.thread_name = "DatabaseManagementTask%s_%s".format(database_information.db_type, op_type)
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
        try:
            self.start_task()
            sirius_client_object = SiriusClient()
            self.error, self.message, self.data = sirius_client_object.database_management(
                op_type=self.op_type,
                database_info=self.database_information,
                operation_config=self.operation_config,
            )

            self.test_log.debug(self.message)

        except Exception as e:
            self.log.critical(e)
        self.set_result(True)
        self.complete_task()

    def get_total_doc_ops(self):
        return self.operation_config.end - self.operation_config.start


class BlobLoadTask(Task):
    def __init__(self, task_manager, op_type, operation_config, external_storage_config,
                 default_sirius_base_url="http://0.0.0.0:4000"):
        """
            :param task_manager: Instance of TaskManager class (Jython_tasks.task_manager.TaskManager)
            :param op_type: Instance of SiriusCodes class (sirius_client_framework.sirius_constants.SiriusCodes)
            :param external_storage_config: Instance of ExternalStorage class (sirius_client_framework.external_storage_config.ExternalStorage)
            :param operation_config: Instance of sirius_client_framework.operation_config.OperationConfig
            :param default_sirius_base_url: string with url and port specification of Sirius Server
        """
        self.thread_name = "BlobLoadTask%s_%s".format(external_storage_config.cloud_provider, op_type)
        super(BlobLoadTask, self).__init__(self.thread_name)
        self.op_type = op_type
        self.sirius_base_url = default_sirius_base_url
        self.operation_config = operation_config
        self.external_storage_config = external_storage_config
        self.task_manager = task_manager
        self.fail = dict()
        self.fail_count = 0
        self.success_count = 0
        self.resultSeed = ""
        self.errors = {}
        self.response = {}
        self.result = False

    def call(self):
        try:
            self.start_task()
            sirius_client_object = SiriusClient()
            self.fail, self.success_count, self.fail_count, self.resultSeed = sirius_client_object.start_blob_workload(
                op_type=self.op_type, operation_config=self.operation_config, external_storage_config=self.external_storage_config)
            self.test_log.debug("success_count : " + str(self.success_count))
            self.test_log.debug("fail_count : " + str(self.fail_count))
            self.test_log.debug("failed_items: " + "".join(self.fail.items()))

        except Exception as e:
            print(e)
            self.log.critical(e)
        self.set_result(True)
        self.complete_task()

    def get_total_doc_ops(self):
        return self.operation_config.end - self.operation_config.start
