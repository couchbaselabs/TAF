'''
Created on 1-February-2024

@author: umang.agrawal
'''
import random

from CbasLib.cbas_entity import ExternalDB
from Goldfish.columnar_base import GoldFishBaseTest
from CbasLib.CBASOperations import CBASHelper
from threading import Thread


class KafkaLinks(GoldFishBaseTest):

    def setUp(self):
        super(KafkaLinks, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.project.instances[0]
        self.cbas_util.cleanup_cbas(self.cluster)

        self.mongo_connection_string = self.input.param("mongo_on_prem_url",
                                                        None)
        self.mongo_db_name = "Kafka_Functional_DB"
        self.mongo_colletion = "Kafka_Functional_Coll"
        self.fully_qualified_mongo_collection_name = [
            ("", CBASHelper.format_name(
                self.mongo_db_name, self.mongo_colletion))]

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        super(KafkaLinks, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def test_create_kafka_link_mongo(self):
        """
        This test will cover all the scenarios related to creating a Kafka
        link to MongoDb.
        Steps -
        1. Create links for all the scenarios.
        2. If there are no failures while creating links, then proceed to
        connect links. This step is required as create link statement only
        creates a metadata entry for the link, there can be scenarios where
        link creation can pass but link connection could fail due to wrong
        configurations.
        3. Disconnect connected links.
        4. Destroy created links.
        """

        testcases = [
            {
                "description": "Create a kafka link with 1 part Name",
                "name_cardinality": 1
            },
            {
                "description": "Create a kafka link with 2 part Name",
                "name_cardinality": 2
            },
            {
                "description": "Create a kafka link with 3 part Name",
                "name_cardinality": 3
            },
            {
                "description": "Create a kafka link without passing source "
                               "DB details",
                "no_source_cluster_detail": {},
                "validate_error_msg": True,
                "expected_error": "Required parameter 'source' not supplied"
            },
            {
                "description": "Create a kafka link with source as mongodb",
                "source": "mongodb",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'mongodb' for kafka link"
            },
            {
                "description": "Create a kafka link with source as MongoDB",
                "source": "MongoDB",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'MongoDB' for kafka link"
            },
            {
                "description": "Create a kafka link with invalid value as "
                               "source",
                "source": "mongo",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'mongo' for kafka link"
            },
            {
                "description": "Create a kafka link with source value type "
                               "other than string",
                "source": 1,
                "validate_error_msg": True,
                "expected_error": "Internal error"
            },
            {
                "description": "Create a kafka link with empty connectionFields",
                "empty_connectionFields": True,
                "validate_error_msg": True,
                "expected_error": "Missing property 'connectionUri' for "
                                  "kafka source 'MONGODB'"
            },
            {
                "description": "Create a kafka link with invalid connectionFields",
                "invalid_connectionFields": True,
                "validate_error_msg": True,
                "expected_error": "Invalid property 'accessKeyId' for kafka "
                                  "source 'MONGODB'"
            },
            {
                "description": "Create a kafka link with name starting with "
                               "reserved keyword Local",
                "use_Local": True,
                "validate_error_msg": True,
                "expected_error": "Invalid link name Local1. Links starting "
                                  "with 'Local' are reserved by the system"
            },
            {
                "description": "Create a kafka link with a very long name",
                "name_length": 260,
                "validate_error_msg": True,
                "expected_error": "Invalid name for a database object"
            },
        ]

        failed_testcases = list()
        links_created = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                mongo_obj = ExternalDB(
                    db_type="mongo",
                    mongo_connection_uri=self.mongo_connection_string
                ).get_source_db_detail_object_for_kafka_links()

                name_cardinality = testcase.get("name_cardinality", 3)
                if testcase.get("non_default_db", False):
                    name_cardinality = 3

                if "no_source_cluster_detail" in testcase:
                    mongo_obj = {}

                if "source" in testcase:
                    mongo_obj["source"] = testcase["source"]

                if testcase.get("invalid_connectionFields", False):
                    mongo_obj["connectionFields"] = {
                        "accessKeyId": "abcdefghijk",
                        "secretAccessKey": "abcdefghijk",
                        "region": "us-east-1"
                    }

                if testcase.get("empty_connectionFields", False):
                    mongo_obj["connectionFields"] = {}

                link = self.cbas_util.create_kafka_link_obj(
                    cluster=self.cluster, dataverse=None, database=None,
                    link_cardinality=name_cardinality,
                    db_type="mongo",
                    external_db_details=mongo_obj,
                    no_of_objs=1, name_length=testcase.get("name_length", 30),
                    fixed_length=True)[0]

                if testcase.get("use_Local", False):
                    link.name = "Local1"

                if testcase.get("name_cardinality", 3) == 3:
                    database = link.database_name
                    dataverse = link.dataverse_name
                elif testcase.get("name_cardinality", 3) == 2:
                    database = None
                    dataverse = link.dataverse_name
                elif testcase.get("name_cardinality", 3) == 1:
                    database = None
                    dataverse = None

                if not self.cbas_util.create_kafka_link(
                        cluster=self.cluster, link_name=link.name,
                        external_db_details=link.external_database_details,
                        dataverse_name=dataverse, database_name=database,
                        validate_error_msg=testcase.get(
                            "validate_error_msg", False),
                        username=None, password=None,
                        expected_error=testcase.get("expected_error", ""),
                        timeout=300, analytics_timeout=300):
                    raise Exception("Error while creating link")

                # Only append links when we don't expect link creation to fail.
                if not testcase.get("validate_error_msg", False):
                    links_created.append(link)
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))
        else:
            def validate_link_metadata(link, result):
                result[link.full_name] = self.cbas_util.validate_link_in_metadata(
                    self.cluster, link.name, link.dataverse_name,
                    link.database_name, link.link_type.upper())

            def connect_link(link, result):
                result[link.full_name] = self.cbas_util.connect_link(
                    self.cluster, link.full_name)

            def disconnect_link(link, result):
                result[link.full_name] = self.cbas_util.disconnect_link(
                    self.cluster, link.full_name)

            def drop_link(link, result):
                result[link.full_name] = self.cbas_util.drop_link(
                    self.cluster, link.full_name, if_exists=True)

            threads = {
                "validate_metadata": [],
                "connect_links": [],
                "disconnect_links": [],
                "drop_links": []
            }
            thread_results = dict()

            for link in links_created:
                threads["validate_metadata"].append(Thread(
                    target=validate_link_metadata,
                    name="validate_metadata_thread",
                    args=(link, thread_results,)
                ))
                threads["connect_links"].append(Thread(
                    target=connect_link,
                    name="connect_links_thread",
                    args=(link, thread_results,)
                ))
                threads["disconnect_links"].append(Thread(
                    target=disconnect_link,
                    name="disconnect_links_thread",
                    args=(link, thread_results,)
                ))
                threads["drop_links"].append(Thread(
                    target=drop_link,
                    name="drop_links_thread",
                    args=(link, thread_results,)
                ))

            # Validating metadata entry for all created links.
            self.start_threads(threads["validate_metadata"])
            metadata_validation_failed = False
            for key, value in thread_results.iteritems():
                if not value:
                    self.log.error("FAILED : Metadata validation for Kafka "
                                   "link {}".format(key))
                    metadata_validation_failed = True
            if metadata_validation_failed:
                self.fail("Failed while validating metadata entry for links")

            # Connecting all the links
            thread_results.clear()
            self.start_threads(threads["connect_links"])
            failed_links = list()
            for link in links_created:
                if not thread_results[link.full_name]:
                    self.log.error("FAILED : Connecting Kafka link {}".format(
                        link.full_name))
                    failed_links.append(link)
            if failed_links:
                self.fail("Failed while connecting links - {}".format(failed_links))

            # Validate links are in connected state
            if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTED", links_created):
                self.fail("Failed while validating link state is CONNECTED.")

            # Disconnecting all the links
            thread_results.clear()
            self.start_threads(threads["disconnect_links"])
            failed_links = list()
            for link in links_created:
                if not thread_results[link.full_name]:
                    self.log.error("FAILED : Disconnecting Kafka link {"
                                   "}".format(link.full_name))
                    failed_links.append(link)
            if failed_links:
                self.fail("Failed while disconnecting links - {}".format(failed_links))

            # Validate links are in disconnected state
            if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "DISCONNECTED", links_created):
                self.fail(
                    "Failed while validating link state is DISCONNECTED.")

            thread_results.clear()
            self.start_threads(threads["drop_links"])
            drop_link_failed = False
            for key, value in thread_results.iteritems():
                if not value:
                    self.log.error("Failed dropping kafka link {}".format(key))
                    drop_link_failed = True
            if drop_link_failed:
                self.fail("Failed while dropping kafka links")

    def test_create_kafka_link_dynamo(self):
        testcases = [
            {
                "description": "Create a kafka link without passing source "
                               "DB details",
                "no_source_cluster_detail": {},
                "validate_error_msg": True,
                "expected_error": "Required parameter 'source' not supplied"
            },
            {
                "description": "Create a kafka link with source as dynamodb",
                "source": "dynamodb",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'dynamodb' for kafka link"
            },
            {
                "description": "Create a kafka link with source as DynamoDB",
                "source": "DynamoDB",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'DynamoDB' for kafka link"
            },
            {
                "description": "Create a kafka link with invalid value as "
                               "source",
                "source": "dynamo",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'dynamo' for kafka link"
            },
            {
                "description": "Create a kafka link with invalid connectionFields",
                "invalid_connectionFields": True,
                "validate_error_msg": True,
                "expected_error": "Invalid property 'databaseHostname' for kafka "
                                  "source 'DYNAMODB'"
            },
            {
                "description": "Create a kafka link with missing accessKeyId "
                               "field in connectionFields",
                "missing_field": "accessKeyId",
                "validate_error_msg": True,
                "expected_error": "Missing property 'accessKeyId' for kafka "
                                  "source 'DYNAMODB'"
            },
            {
                "description": "Create a kafka link with missing secretAccessKey "
                               "field in connectionFields",
                "missing_field": "secretAccessKey",
                "validate_error_msg": True,
                "expected_error": "Missing property 'secretAccessKey' for "
                                  "kafka source 'DYNAMODB'"
            },
            {
                "description": "Create a kafka link with missing region "
                               "field in connectionFields",
                "missing_field": "region",
                "validate_error_msg": True,
                "expected_error": "Missing property 'region' for kafka "
                                  "source 'DYNAMODB'"
            },
            {
                "description": "Create a kafka link with empty connectionFields",
                "missing_field": "all",
                "validate_error_msg": True,
                "expected_error": "Missing property 'accessKeyId' for "
                                  "kafka source 'DYNAMODB'"
            },
        ]

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                dynamo_obj = ExternalDB(
                    db_type="dynamo",
                    dynamo_access_key=self.input.param("aws_access_key"),
                    dynamo_secret_key=self.input.param("aws_secret_key"),
                    dynamo_region="us-west-1"
                    ).get_source_db_detail_object_for_kafka_links()

                if "no_source_cluster_detail" in testcase:
                    dynamo_obj = {}

                if "source" in testcase:
                    dynamo_obj["source"] = testcase["source"]

                if testcase.get("invalid_connectionFields", False):
                    dynamo_obj["connectionFields"] = {
                        "databaseHostname": "abcdefghijk",
                        "secretAccessKey": "abcdefghijk",
                        "region": "us-east-1"
                    }

                if "missing_field" in testcase:
                    if testcase["missing_field"] == "accessKeyId":
                        del(dynamo_obj["connectionFields"]["accessKeyId"])
                    elif testcase["missing_field"] == "secretAccessKey":
                        del(dynamo_obj["connectionFields"]["secretAccessKey"])
                    elif testcase["missing_field"] == "region":
                        del(dynamo_obj["connectionFields"]["region"])
                    elif testcase["missing_field"] == "all":
                        dynamo_obj["connectionFields"] = {}

                link = self.cbas_util.create_kafka_link_obj(
                    cluster=self.cluster, dataverse=None, database=None,
                    link_cardinality=3, db_type="dynamo",
                    external_db_details=dynamo_obj,
                    no_of_objs=1, name_length=30, fixed_length=True)[0]

                if not self.cbas_util.create_kafka_link(
                        cluster=self.cluster, link_name=link.name,
                        external_db_details=link.external_database_details,
                        dataverse_name=link.dataverse_name,
                        database_name=link.database_name,
                        validate_error_msg=testcase.get(
                            "validate_error_msg", False),
                        expected_error=testcase.get("expected_error", "")):
                    raise Exception("Error while creating link")

            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_create_kafka_link_mysql(self):
        testcases = [
            {
                "description": "Create a kafka link without passing source "
                               "DB details",
                "no_source_cluster_detail": {},
                "validate_error_msg": True,
                "expected_error": "Required parameter 'source' not supplied"
            },
            {
                "description": "Create a kafka link with source as mysqldb",
                "source": "mysqldb",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'mysqldb' for kafka link"
            },
            {
                "description": "Create a kafka link with source as MySQLDB",
                "source": "MySQLDB",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'MySQLDB' for kafka link"
            },
            {
                "description": "Create a kafka link with invalid value as "
                               "source",
                "source": "MYSQL",
                "validate_error_msg": True,
                "expected_error": "Invalid source 'MYSQL' for kafka link"
            },
            {
                "description": "Create a kafka link with invalid "
                               "connectionFields",
                "invalid_connectionFields": True,
                "validate_error_msg": True,
                "expected_error": "Invalid property 'accessKeyId' for kafka "
                                  "source 'MYSQLDB'"
            },
            {
                "description": "Create a kafka link with missing "
                               "databaseHostname field in connectionFields",
                "missing_field": "databaseHostname",
                "validate_error_msg": True,
                "expected_error": "Missing property 'databaseHostname' for "
                                  "kafka source 'MYSQLDB'"
            },
            {
                "description": "Create a kafka link with missing databasePort "
                               "field in connectionFields",
                "missing_field": "databasePort",
                "validate_error_msg": True,
                "expected_error": "Missing property 'databasePort' for kafka "
                                  "source 'MYSQLDB'"
            },
            {
                "description": "Create a kafka link with missing databaseUser "
                               "field in connectionFields",
                "missing_field": "databaseUser",
                "validate_error_msg": True,
                "expected_error": "Missing property 'databaseUser' for kafka "
                                  "source 'MYSQLDB'"
            },
            {
                "description": "Create a kafka link with missing "
                               "databasePassword field in connectionFields",
                "missing_field": "databasePassword",
                "validate_error_msg": True,
                "expected_error": "Missing property 'databasePassword' for "
                                  "kafka source 'MYSQLDB'"
            },
            {
                "description": "Create a kafka link with missing "
                               "databaseServerId field in connectionFields",
                "missing_field": "databaseServerId",
                "validate_error_msg": True,
                "expected_error": "Missing property 'databaseServerId' for "
                                  "kafka source 'MYSQLDB'"
            },
            {
                "description": "Create a kafka link with empty connectionFields",
                "missing_field": "all",
                "validate_error_msg": True,
                "expected_error": "Missing property 'databaseUser' for "
                                  "kafka source 'MYSQLDB'"
            },
        ]

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                mysql_obj = ExternalDB(
                    db_type="rds", rds_hostname="", rds_username="",
                    rds_password="", rds_port=123, rds_server_id=1
                    ).get_source_db_detail_object_for_kafka_links()

                if "no_source_cluster_detail" in testcase:
                    mysql_obj = {}

                if "source" in testcase:
                    mysql_obj["source"] = testcase["source"]

                if testcase.get("invalid_connectionFields", False):
                    mysql_obj["connectionFields"] = {
                    "accessKeyId": "invalid",
                    "databasePort": "invalid",
                    "databaseUser": "invalid",
                    "databasePassword": "invalid",
                    "databaseServerId": "invalid",
                }

                if "missing_field" in testcase:
                    if testcase["missing_field"] == "databaseHostname":
                        del(mysql_obj["connectionFields"]["databaseHostname"])
                    elif testcase["missing_field"] == "databasePort":
                        del(mysql_obj["connectionFields"]["databasePort"])
                    elif testcase["missing_field"] == "databaseUser":
                        del(mysql_obj["connectionFields"]["databaseUser"])
                    elif testcase["missing_field"] == "databasePassword":
                        del(mysql_obj["connectionFields"]["databasePassword"])
                    elif testcase["missing_field"] == "databaseServerId":
                        del(mysql_obj["connectionFields"]["databaseServerId"])
                    elif testcase["missing_field"] == "all":
                        mysql_obj["connectionFields"] = {}

                link = self.cbas_util.create_kafka_link_obj(
                    cluster=self.cluster, dataverse=None, database=None,
                    link_cardinality=3, db_type="rds",
                    external_db_details=mysql_obj,
                    no_of_objs=1, name_length=30, fixed_length=True)[0]

                if not self.cbas_util.create_kafka_link(
                        cluster=self.cluster, link_name=link.name,
                        external_db_details=link.external_database_details,
                        dataverse_name=link.dataverse_name,
                        database_name=link.database_name,
                        validate_error_msg=testcase.get(
                            "validate_error_msg", False),
                        expected_error=testcase.get("expected_error", "")):
                    raise Exception("Error while creating link")

            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_connect_kafka_links(self):
        testcases = [
            {
                "description": "Connect a kafka link with 3 part Name",
                "name_cardinality": 3
            },
            {
                "description": "Connect a kafka link with 1 part Name",
                "name_cardinality": 1
            },
            {
                "description": "Connect a kafka link with 2 part Name",
                "name_cardinality": 2
            },
            {
                "description": "Connect a kafka link with 2 part Name, "
                               "when link is not present in Default database",
                "name_cardinality": 2,
                "non_default_db": True,
                "validate_error_msg": True,
                "expected_error": "Cannot find analytics scope with name "
                                  "Default"
            }
        ]

        failed_testcases = list()
        link_created = None

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                mongo_obj = ExternalDB(
                    db_type="mongo",
                    mongo_connection_uri=self.mongo_connection_string
                ).get_source_db_detail_object_for_kafka_links()

                name_cardinality = testcase.get("name_cardinality", 3)
                if testcase.get("non_default_db", False):
                    name_cardinality = 3

                link = self.cbas_util.create_kafka_link_obj(
                    cluster=self.cluster, dataverse=None, database=None,
                    link_cardinality=name_cardinality,
                    db_type="mongo",
                    external_db_details=mongo_obj,
                    no_of_objs=1, fixed_length=False)[0]

                if not self.cbas_util.create_kafka_link(
                        cluster=self.cluster, link_name=link.name,
                        external_db_details=link.external_database_details,
                        dataverse_name=link.dataverse_name,
                        database_name=link.database_name):
                    self.fail("Error while creating link")

                if testcase.get("name_cardinality", 3) == 3:
                    link_name = ".".join([link.database_name,
                                          link.dataverse_name, link.name])
                elif testcase.get("name_cardinality", 3) == 2:
                    link_name = ".".join([link.dataverse_name, link.name])
                elif testcase.get("name_cardinality", 3) == 1:
                    link_name = link.name

                if not self.cbas_util.connect_link(
                        self.cluster, link_name,
                        validate_error_msg=testcase.get("validate_error_msg",
                                                        False),
                        expected_error=testcase.get("expected_error", "")
                ):
                    raise Exception("Error while connecting link")

                if (not link_created) and (not testcase.get(
                        "validate_error_msg", False)):
                    link_created = link
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following tests failed - {}".format(failed_testcases))

        # Connect a link when it is in config_updated state
        dataset_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_standalone_collection_using_links(
                self.cluster, dataset_name, primary_key={"_id": "string"},
                link_name=link_created.full_name,
                external_collection=self.fully_qualified_mongo_collection_name[
                    0][1]):
            self.fail("Error while creating standalone collection on kafka "
                      "link")

        self.log.info(
            "Test - Verify connecting link which is in CONFIG_UPDATED state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONFIG_UPDATED", [link_created]):
            self.fail("CONFIG_UPDATED state was not seen for Link {"
                      "}".format(link_created.full_name))

        if not self.cbas_util.connect_link(
                self.cluster, link_created.full_name, validate_error_msg=True,
                expected_error="Invalid state found for Kafka Link '{0}' "
                               "expected: 'DISCONNECTED,RETRY_FAILED' actual: "
                               "'CONFIG_UPDATED'".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify connecting link which is in "
                "CONFIG_UPDATED state")

        self.log.info(
            "Test - Verify connecting link which is in CONNECTING state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTING", [link_created]):
            self.fail("CONNECTING state was not seen for Link {"
                      "}".format(link_created.full_name))

        if not self.cbas_util.connect_link(
                self.cluster, link_created.full_name, validate_error_msg=True,
                expected_error="Invalid state found for Kafka Link '{0}' "
                               "expected: 'DISCONNECTED,RETRY_FAILED' actual: "
                               "'CONNECTING'".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify connecting link which is in CONNECTING "
                "state")

        # Connect an already connected link.
        self.log.info(
            "Test - Verify connecting link which is in CONNECTED state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTED", [link_created]):
            self.fail("Few Kafka Links were not able to connect")
        if not self.cbas_util.connect_link(
                self.cluster, link_created.full_name,
                validate_error_msg=True,
                expected_error="Invalid state found for Kafka Link '{0}' "
                               "expected: 'DISCONNECTED,RETRY_FAILED' actual: "
                               "'CONNECTED'".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify connecting link which is in CONNECTED "
                "state")

        self.log.info(
            "Test - Verify connecting link which is in DISCONNECTING state")
        if not self.cbas_util.disconnect_link(
                self.cluster, link_created.full_name):
            self.fail("Error while disconnecting link {}".format(
                link_created.full_name))

        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "DISCONNECTING", [link_created]):
            self.fail("DISCONNECTING state was not seen for Link {}".format(
                link_created.full_name))

        if not self.cbas_util.connect_link(
                self.cluster, link_created.full_name,
                validate_error_msg=True,
                expected_error="Invalid state found for Kafka Link '{0}' "
                               "expected: 'DISCONNECTED,RETRY_FAILED' actual: "
                               "'DISCONNECTING'".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify connecting link which is in "
                "DISCONNECTING state")

    def test_disconnect_kafka_links(self):
        testcases = [
            {
                "description": "Disconnect a kafka link with 1 part Name",
                "name_cardinality": 1
            },
            {
                "description": "Disconnect a kafka link with 2 part Name",
                "name_cardinality": 2
            },
            {
                "description": "Disconnect a kafka link with 2 part Name, "
                               "when link is not present in Default database",
                "name_cardinality": 2,
                "non_default_db": True,
                "validate_error_msg": True,
                "expected_error": "Cannot find analytics scope with name "
                                  "Default"
            },
            {
                "description": "Disconnect a kafka link with 3 part Name",
                "name_cardinality": 3
            }
        ]

        link_created = None
        failed_testcases = list()
        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                mongo_obj = ExternalDB(
                    db_type="mongo",
                    mongo_connection_uri=self.mongo_connection_string
                ).get_source_db_detail_object_for_kafka_links()

                name_cardinality = testcase.get("name_cardinality", 3)
                if testcase.get("non_default_db", False):
                    name_cardinality = 3

                link = self.cbas_util.create_kafka_link_obj(
                    cluster=self.cluster, dataverse=None, database=None,
                    link_cardinality=name_cardinality,
                    db_type="mongo",
                    external_db_details=mongo_obj,
                    no_of_objs=1, fixed_length=False)[0]

                if not self.cbas_util.create_kafka_link(
                        cluster=self.cluster, link_name=link.name,
                        external_db_details=link.external_database_details,
                        dataverse_name=link.dataverse_name,
                        database_name=link.database_name):
                    self.fail("Error while creating link")

                if not self.cbas_util.connect_link(self.cluster, link.full_name):
                    raise Exception("Error while connecting link")

                if testcase.get("name_cardinality", 3) == 3:
                    link_name = ".".join([link.database_name,
                                          link.dataverse_name, link.name])
                elif testcase.get("name_cardinality", 3) == 2:
                    link_name = ".".join([link.dataverse_name, link.name])
                elif testcase.get("name_cardinality", 3) == 1:
                    link_name = link.name

                if not self.cbas_util.disconnect_link(
                        self.cluster, link_name,
                        validate_error_msg=testcase.get("validate_error_msg",
                                                        False),
                        expected_error=testcase.get("expected_error", "")
                ):
                    raise Exception("Error while disconnecting link")

                if testcase.get("validate_error_msg", False):
                    link_created = link

            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following tests failed - {}".format(failed_testcases))

        link = link_created

        dataset_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_standalone_collection_using_links(
                self.cluster, dataset_name, primary_key={"_id": "string"},
                link_name=link.full_name,
                external_collection=self.fully_qualified_mongo_collection_name[
                    0][1]):
            self.fail("Error while creating standalone collection on kafka "
                      "link")

        self.log.info(
            "Test - Verify disconnecting link which is in CONFIG_UPDATED state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONFIG_UPDATED", [link]):
            self.fail("Kafka Link {} state did not change to "
                      "CONFIG_UPDATED".format(link.full_name))

        if not self.cbas_util.disconnect_link(
                self.cluster, link.full_name, validate_error_msg=True,
                expected_error="Invalid state found for Kafka Link '{0}' "
                               "expected: 'CONNECTED' actual: "
                               "'CONFIG_UPDATED'".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify disconnecting link which is in "
                "CONFIG_UPDATED state")

        self.log.info(
            "Test - Verify disconnecting link which is in CONNECTING state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTING", [link]):
            self.fail("Kafka Link {} state did not change to "
                      "CONNECTING".format(link.full_name))

        if not self.cbas_util.disconnect_link(
                self.cluster, link.full_name, validate_error_msg=True,
                expected_error="Invalid state found for Kafka Link '{0}' "
                               "expected: 'CONNECTED' actual: "
                               "'CONNECTING'".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify disconnecting link which is in "
                "CONNECTING state")

        self.log.info(
            "Test - Verify disconnecting link which is in CONNECTED state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTED", [link]):
            self.fail("Kafka Link {} state did not change to "
                      "CONNECTED".format(link.full_name))

        if not self.cbas_util.disconnect_link(self.cluster, link.full_name):
            self.fail(
                "Test Failed - Verify disconnecting link which is in "
                "CONNECTED state")

        self.log.info(
            "Test - Verify disconnecting link which is in DISCONNECTING state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "DISCONNECTING", [link]):
            self.fail("Kafka Link {} state did not change to "
                      "DISCONNECTING".format(link.full_name))

        if not self.cbas_util.disconnect_link(
                self.cluster, link.full_name, validate_error_msg=True,
                expected_error="Invalid state found for Kafka Link '{0}' "
                               "expected: 'CONNECTED' actual: "
                               "'DISCONNECTING'".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify disconnecting link which is in "
                "DISCONNECTING state")

        self.log.info(
            "Test - Verify disconnecting link which is in DISCONNECTED state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "DISCONNECTED", [link]):
            self.fail("Kafka Link {} state did not change to "
                      "DISCONNECTED".format(link.full_name))

        if not self.cbas_util.disconnect_link(
                self.cluster, link.full_name, validate_error_msg=True,
                expected_error="Invalid state found for Kafka Link '{0}' "
                               "expected: 'CONNECTED' actual: "
                               "'DISCONNECTED'".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify disconnecting link which is in "
                "DISCONNECTED state")

    def test_drop_kafka_links(self):
        testcases = [
            {
                "description": "Drop a kafka link with 1 part Name",
                "name_cardinality": 1
            },
            {
                "description": "Drop a kafka link with 2 part Name",
                "name_cardinality": 2
            },
            {
                "description": "Drop a kafka link with 2 part Name, "
                               "when link is not present in Default database",
                "name_cardinality": 2,
                "non_default_db": True,
                "validate_error_msg": True,
                "expected_error": "Cannot find analytics scope with name "
                                  "Default"
            },
            {
                "description": "Drop a kafka link with 3 part Name",
                "name_cardinality": 3
            }
        ]

        link_created = None
        failed_testcases = list()
        for testcase in testcases:
            try:
                self.log.info(testcase["description"])
                mongo_obj = ExternalDB(
                    db_type="mongo",
                    mongo_connection_uri=self.mongo_connection_string
                ).get_source_db_detail_object_for_kafka_links()

                name_cardinality = testcase.get("name_cardinality", 3)

                if testcase.get("non_default_db", False):
                    name_cardinality = 3

                link = self.cbas_util.create_kafka_link_obj(
                    cluster=self.cluster, dataverse=None, database=None,
                    link_cardinality=name_cardinality,
                    db_type="mongo",
                    external_db_details=mongo_obj,
                    no_of_objs=1, fixed_length=True)[0]

                if not self.cbas_util.create_kafka_link(
                        cluster=self.cluster, link_name=link.name,
                        external_db_details=link.external_database_details,
                        dataverse_name=link.dataverse_name,
                        database_name=link.database_name):
                    self.fail("Error while creating link")

                if testcase.get("name_cardinality", 3) == 3:
                    link_name = ".".join([link.database_name,
                                          link.dataverse_name, link.name])
                elif testcase.get("name_cardinality", 3) == 2:
                    link_name = ".".join([link.dataverse_name, link.name])
                elif testcase.get("name_cardinality", 3) == 1:
                    link_name = link.name

                if not self.cbas_util.drop_link(
                        self.cluster, link_name,
                        validate_error_msg=testcase.get(
                            "validate_error_msg", False),
                        expected_error=testcase.get("expected_error", "")):
                    raise Exception("Error while dropping link {}".format(
                        link_name))
                if testcase.get("validate_error_msg", False):
                    link_created = link
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following tests failed - {}".format(failed_testcases))

        link = link_created

        dataset_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_standalone_collection_using_links(
                self.cluster, dataset_name, primary_key={"_id": "string"},
                link_name=link.full_name,
                external_collection=self.fully_qualified_mongo_collection_name[
                    0][1]):
            self.fail("Error while creating standalone collection on kafka "
                      "link")

        if not self.cbas_util.connect_link(self.cluster, link.full_name):
            self.fail("Error while connecting link {}".format(link.full_name))

        self.log.info("Test - Verify dropping link which is in CONNECTING state")
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTING", [link]):
            self.fail("Kafka Link {} state did not change to "
                      "CONNECTING".format(link.full_name))

        if not self.cbas_util.drop_link(
                self.cluster, link.full_name, validate_error_msg=True,
                expected_error="Link {0} cannot be dropped because it has "
                               "the following analytics collections".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify dropping link which is in CONNECTED "
                "state")

        # Wait for all the links above to be in connected state
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTED", [link]):
            self.fail("Few Kafka Links were not able to connect")

        self.log.info(
            "Test - Verify dropping link which is in CONFIG_UPDATED state")
        if not self.cbas_util.drop_dataset(self.cluster, dataset_name):
            self.fail("Dropping standalone collection {0} failed".format(
                dataset_name))
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONFIG_UPDATED", [link]):
            self.fail("CONFIG_UPDATED state was not seen for Link {"
                      "}".format(link.full_name))

        if not self.cbas_util.drop_link(self.cluster, link.full_name):
            self.fail(
                "Test Failed - Verify dropping link which is in CONFIG_UPDATED "
                "state")

        def setup_link():
            mongo_obj = ExternalDB(
                db_type="mongo",
                mongo_connection_uri=self.mongo_connection_string
            ).get_source_db_detail_object_for_kafka_links()

            link = self.cbas_util.create_kafka_link_obj(
                cluster=self.cluster, dataverse=None, database=None,
                link_cardinality=3, db_type="mongo", external_db_details=mongo_obj,
                no_of_objs=1, name_length=30, fixed_length=True)[0]

            if not self.cbas_util.create_kafka_link(
                    cluster=self.cluster, link_name=link.name,
                    external_db_details=link.external_database_details,
                    dataverse_name=link.dataverse_name,
                    database_name=link.database_name):
                self.fail("Error while creating link")
            if not self.cbas_util.connect_link(self.cluster, link.full_name):
                self.fail(
                    "Error while connecting link {}".format(link.full_name))
            return link

        link = setup_link()

        # Wait for all the links above to be in connected state
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTED", [link]):
            self.fail("Few Kafka Links were not able to connect")

        self.log.info(
            "Test - Verify dropping link which is in CONNECTED state")
        if not self.cbas_util.drop_link(
                self.cluster, link.full_name):
            self.fail(
                "Test Failed - Verify dropping link which is in CONNECTED state")

        link = setup_link()
        dataset_name = self.cbas_util.generate_name()
        if not self.cbas_util.create_standalone_collection_using_links(
                self.cluster, dataset_name, primary_key={"_id": "string"},
                link_name=link.full_name,
                external_collection=self.fully_qualified_mongo_collection_name[
                    0][1]):
            self.fail("Error while creating standalone collection on kafka "
                      "link")

        # Wait for all the links above to be in connected state
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "CONNECTED", [link]):
            self.fail("Few Kafka Links were not able to connect")

        self.log.info(
            "Test - Verify dropping link which is in CONNECTED state when "
            "there is a dataset associated with the link")
        if not self.cbas_util.drop_link(
                self.cluster, link.full_name, validate_error_msg=True,
                expected_error="Link {0} cannot be dropped because it has "
                               "the following analytics collections".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify dropping link which is in CONNECTED "
                "state when there is a dataset associated with the link")

        self.log.info(
            "Test - Verify dropping link which is in DISCONNECTING state")
        if not self.cbas_util.disconnect_link(self.cluster, link.full_name):
            self.fail("Error while disconnecting link {}".format(link.full_name))
        if not self.cbas_util.drop_link(
                self.cluster, link.full_name, validate_error_msg=True,
                expected_error="Link {0} cannot be dropped because it has "
                               "the following analytics collections".format(
                    self.cbas_util.unformat_name(link.full_name))):
            self.fail(
                "Test Failed - Verify dropping link which is in DISCONNECTING state")

            # Wait for all the links above to be in connected state
        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, "DISCONNECTED", [link]):
            self.fail("Few Kafka Links were not able to disconnect")

        self.log.info(
            "Test - Verify Metadata entry for dropped link")
        if self.cbas_util.validate_link_in_metadata(
                self.cluster, link.name, link.dataverse_name,
                link.database_name, link.link_type.upper()):
            self.fail(
                "Test Failed - Verify Metadata entry for dropped link")

    def test_create_standalone_collection_on_kafka_link(self):
        testcases = [
            {
                "description": "Create a standalone collection using "
                               "`create DATASET` format",
                "ddl_format": "DATASET"
            },
            {
                "description": "Create a standalone collection using "
                               "`create ANALYTICS COLLECTION` format",
                "ddl_format": "ANALYTICS COLLECTION"
            },
            {
                "description": "Create a standalone collection using "
                               "`create COLLECTION` format",
                "ddl_format": "COLLECTION"
            },
            {
                "description": "Create a standalone collection using "
                               "if_not_exists in DDL",
                "if_not_exists": True
            },
            {
                "description": "Create a standalone collection with "
                               "autogenerated primary key",
                "if_not_exists": True
            },
            {
                "description": "Create a standalone collection without "
                               "primary key",
                "if_not_exists": True
            },
            {
                "description": "Create a standalone collection on "
                               "non-existent source collection",
                "if_not_exists": True
            },
            {
                "description": "Create a standalone collection using "
                               "non-existent link",
                "if_not_exists": True
            },
            {
                "description": "Create a standalone collection using "
                               "non-kafka link",
                "if_not_exists": True
            },
            {
                "description": "Create multiple standalone collection on "
                               "same source collection using same link",
                "if_not_exists": True
            },
            {
                "description": "Create multiple standalone collection on "
                               "same source collection using multiple links",
                "if_not_exists": True
            },
            {
                "description": "Create a standalone collection with where "
                               "clause",
                "if_not_exists": True
            },
        ]

        mongo_obj = ExternalDB(
            db_type="mongo",
            mongo_connection_uri=self.mongo_connection_string
        ).get_source_db_detail_object_for_kafka_links()

        links = self.cbas_util.create_kafka_link_obj(
            cluster=self.cluster, dataverse=None, database=None,
            link_cardinality=3, db_type="mongo",
            external_db_details=mongo_obj,
            no_of_objs=2, name_length=30, fixed_length=True)

        for link in links:
            if not self.cbas_util.create_kafka_link(
                    cluster=self.cluster, link_name=link.name,
                    external_db_details=link.external_database_details,
                    dataverse_name=link.dataverse_name,
                    database_name=link.database_name):
                raise Exception("Error while creating link")

        failed_testcases = list()
        datasets = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                dataset_obj = self.cbas_util.create_standalone_dataset_obj(
                    self.cluster, no_of_objs=1,
                    datasource="mongo", primary_key=pk, link=link,
                    external_collection_name=self.fully_qualified_mongo_collection_name[0][1])[0]

                if not self.cbas_util.create_standalone_collection_using_links(
                    self.cluster, dataset_obj.name,
                    ddl_format=testcase.get("ddl_format", "random"),
                    if_not_exists=testcase.get("if_not_exists", False),
                    dataverse_name=dataset_obj.dataverse_name,
                    database_name=dataset_obj.database_name,
                    primary_key=dataset_obj.primary_key,
                    link_name=dataset_obj.link_name,
                    external_collection=dataset_obj.external_collection_name,
                    validate_error_msg=testcase.get("validate_error_msg",
                                                    False),
                        expected_error=testcase.get("expected_error", False)):
                    raise Exception("Error while verifying test {0}".format(
                        testcase["description"]))

                if not testcase.get("validate_error_msg", False):
                    datasets.append(dataset_obj)
            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))



