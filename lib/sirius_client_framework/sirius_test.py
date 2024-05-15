from cb_basetest import CouchbaseBaseTest
from sirius_client_framework.external_storage_config import AwsS3
from sirius_client_framework.multiple_database_config import \
    DynamoDBLoader, \
    CassandraLoader, \
    CouchbaseLoader, \
    MongoLoader, \
    MySQLLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from Jython_tasks.sirius_task import \
    WorkLoadTask, DatabaseManagementTask, BlobLoadTask
from sirius_client_framework.sirius_constants import SiriusCodes


class SiriusTest(CouchbaseBaseTest):
    def setUp(self):
        super(SiriusTest, self).setUp()
        self.mongo_username = self.input.param("mongo_user", "Administrator")
        self.mongo_password = self.input.param("mongo_password", "password")
        self.mongo_connection_string = \
            self.input.param("mongo_url", "mongodb://127.0.0.1:27017")
        self.couchbase_username = self.input.param("couchbase_user", "Admin")
        self.couchbase_password = \
            self.input.param("couchbase_password", "password")
        self.couchbase_connection_string = \
            self.input.param("couchbase_url", "couchbases://127.0.0.1:8091")
        self.aws_access_key_id = self.input.param("aws_access_key_id", "<KEY>")
        self.aws_secret_access_key = \
            self.input.param("aws_secret_access_key", "<KEY>")
        self.aws_region = self.input.param("aws_region", "us-west-1")
        self.cassandra_username = \
            self.input.param("cassandra_user", "Administrator")
        self.cassandra_password = \
            self.input.param("cassandra_password", "password")
        self.cassandra_connection_string = \
            self.input.param("cassandra_url", "cassandra://127.0.0.1:9091")
        self.dynamo_username = self.input.param("dynamo_user", "Administrator")
        self.dynamo_password = self.input.param("dynamo_password", "password")
        self.dynamo_connection_string = \
            self.input.param("dynamo_url", "dynamo://127.0.0.1:8091")
        self.mysql_username = self.input.param("mysql_user", "Administrator")
        self.mysql_password = self.input.param("mysql_password", "password")
        self.mysql_connection_string = \
            self.input.param("mysql_url", "mysql://127.0.0.1:8091")
        self.sirius_base_url = "http://127.0.0.1:4000"
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        super(SiriusTest, self).tearDown()

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def insert_mongo_doc(self):
        database_information = MongoLoader(
            username=self.mongo_username,
            password=self.mongo_password,
            connection_string=self.mongo_connection_string,
            collection="Sirius",
            database="SiriusTestCollection",
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1000,
            template=SiriusCodes.Templates.PERSON,
            doc_size=1024,
        )
        task_insert = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)

    def upsert_mongo_doc(self):
        database_information = MongoLoader(
            username=self.mongo_username,
            password=self.mongo_password,
            connection_string=self.mongo_connection_string,
            collection="Sirius",
            database="SiriusTestCollection",
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1000,
            template=SiriusCodes.Templates.PERSON,
            doc_size=1024,
        )
        task_upsert = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_UPDATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_upsert)
        self.task_manager.get_task_result(task_upsert)
        
    def insert_couchbase_doc(self):
        database_information = CouchbaseLoader(
            username=self.couchbase_username,
            password=self.couchbase_password,
            connection_string=self.couchbase_connection_string,
            bucket="k1",
            scope="_default",
            collection="_default",
            sdk_batch_size=200,
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1000,
            template=SiriusCodes.Templates.PERSON,
            doc_size=1024,
        )
        task_insert = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)

    def upsert_couchbase_doc(self):
        database_information = CouchbaseLoader(
            username=self.couchbase_username,
            password=self.couchbase_password,
            connection_string=self.couchbase_connection_string,
            bucket="k1",
            scope="_default",
            collection="_default",
            sdk_batch_size=200,
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1000,
            template=SiriusCodes.Templates.PERSON,
            doc_size=1024,
        )
        task_upsert = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.BULK_UPDATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_upsert)
        self.task_manager.get_task_result(task_upsert)
        
    def create_mongo_collection(self):
        database_information = MongoLoader(
            username=self.mongo_username,
            password=self.mongo_password,
            connection_string=self.mongo_connection_string,
            database="SiriusTestCollection",
            collection="demo1"
        )
        operation_config = WorkloadOperationConfig(
            template=SiriusCodes.Templates.PERSON
        )
        task_create = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_create)
        self.task_manager.get_task_result(task_create)

    def createS3Bucket(self):
        external_storage_config = AwsS3(
            aws_access_key=self.aws_access_key_id,
            aws_secret_key=self.aws_secret_access_key,
            aws_region=self.aws_region,
            aws_session_token="empty",
            bucket="sample-sirius-taf",
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1,
            template=SiriusCodes.Templates.PERSON,
        )
        task_create_s3 = BlobLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.S3MgmtOps.CREATE,
            external_storage_config=external_storage_config,
            operation_config=operation_config
        )
        self.task_manager.add_new_task(task_create_s3)
        self.task_manager.get_task_result(task_create_s3)

    def upload_S3_files(self):
        external_storage_config = AwsS3(
            aws_access_key=self.aws_access_key_id,
            aws_secret_key=self.aws_secret_access_key,
            aws_region=self.aws_region,
            aws_session_token="empty",
            bucket="sample-sirius-taf",
            file_format="json",
            file_path="temp/person.json",
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1000,
            template=SiriusCodes.Templates.SMALL,
            doc_size=1024,
        )
        task_insert = BlobLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.S3DocOps.CREATE,
            external_storage_config=external_storage_config,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)

    def create_cassandra_table(self):
        database_information = CassandraLoader(
            username=self.cassandra_username,
            password=self.cassandra_password,
            connection_string=self.cassandra_connection_string,
            keyspace="sirius",
            table="hotels",
        )
        operation_config = WorkloadOperationConfig(
            template=SiriusCodes.Templates.HOTEL,
        )
        task_create = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_create)
        self.task_manager.get_task_result(task_create)

    def insert_cassandra_doc(self):
        database_information = CassandraLoader(
            username=self.cassandra_username,
            password=self.cassandra_password,
            connection_string=self.cassandra_connection_string,
            keyspace="sirius",
            table="hotels",
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1000,
            template=SiriusCodes.Templates.HOTEL,
            doc_size=1024,
        )
        task_insert = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)

    def create_mysql_table(self):
        database_information = MySQLLoader(
            username=self.mysql_username,
            password=self.mysql_password,
            connection_string=self.mysql_connection_string,
            database="sirius",
            table="demo102",
        )
        operation_config = WorkloadOperationConfig(
            template=SiriusCodes.Templates.HOTEL_SQL,
            doc_size=1024,
        )
        task_create = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_create)
        self.task_manager.get_task_result(task_create)

    def insert_mysql_doc(self):
        database_information = MySQLLoader(
            username=self.mysql_username,
            password=self.mysql_password,
            connection_string=self.mysql_connection_string,
            database="sirius",
            table="demo",
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1000,
            template=SiriusCodes.Templates.PERSON_SQL,
            doc_size=1024,
        )
        task_insert = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)

    def create_dynamo_table(self):
        database_information = DynamoDBLoader(
            username=self.dynamo_username,
            password=self.dynamo_password,
            connection_string=self.dynamo_connection_string,
            table="sirius-taf-2k",
        )
        operation_config = WorkloadOperationConfig(
            template=SiriusCodes.Templates.PERSON,
        )
        task_create = DatabaseManagementTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DBMgmtOps.CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_create)
        self.task_manager.get_task_result(task_create)

    def insert_dynamo_doc(self):
        database_information = DynamoDBLoader(
            username=self.dynamo_username,
            password=self.dynamo_password,
            connection_string=self.dynamo_connection_string,
            table="sirius-taf-2k",
        )
        operation_config = WorkloadOperationConfig(
            start=0,
            end=1000,
            template=SiriusCodes.Templates.PERSON,
            doc_size=1024,
        )
        task_insert = WorkLoadTask(
            task_manager=self.task_manager,
            op_type=SiriusCodes.DocOps.CREATE,
            database_information=database_information,
            operation_config=operation_config,
        )
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)

    def test_run_sirius(self):
        """
        Main test function
        """
        self.log.debug("creating mongodb collection")
        self.create_mongo_collection()
        self.log.debug("insert load : mongodb")
        self.insert_mongo_doc()
        self.log.debug("upsert load : mongodb")
        self.upsert_mongo_doc()
        self.log.debug("insert load : couchbase")
        self.insert_couchbase_doc()
        self.log.debug("upsert load : couchbase")
        self.upsert_couchbase_doc()
        # self.createS3Bucket()
        # self.upload_S3_files()
        self.log.debug("create cassandra table")
        self.create_cassandra_table()
        self.log.debug("insert load : cassandra")
        self.insert_cassandra_doc()
        self.log.debug("create mysql table")
        self.create_mysql_table()
        self.log.debug("insert load : mysql")
        self.insert_mysql_doc()
        self.log.debug("create dynamo table")
        self.create_dynamo_table()
        self.log.debug("insert load : dynamo")
        self.insert_dynamo_doc()
