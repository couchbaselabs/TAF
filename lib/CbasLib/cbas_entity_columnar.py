"""
Created on 08-Dec-2020

@author: Umang
"""

from CbasLib.CBASOperations import CBASHelper
from copy import deepcopy


class DatabaseUser(object):
    """
    Database user
    """
    def __init__(self, id, name, password):
        self.id = id
        self.name = name
        self.password = password

    def __str__(self):
        return self.id

    def to_dict(self):
        return {"id": self.id, "name": self.name, "password": self.password}


class ColumnarRole(object):
    """
    Analytics Role.
    """
    def __init__(self, role_name):
        self.role_name = CBASHelper.format_name(role_name)

    def __str__(self):
        return self.role_name


class Database(object):
    """
    Database object
    """

    def __init__(self, name="Default"):
        self.name = CBASHelper.format_name(name)

        self.dataverses = dict()
        self.crud_dataverses = dict()
        if self.name == "Default":
            self.dataverses["Default"] = Dataverse()

    def __str__(self):
        return self.name


class Dataverse(object):
    """
    Dataverse object
    """

    def __init__(self, name="Default", database_name="Default"):
        self.name = CBASHelper.format_name(name)
        self.database_name = CBASHelper.format_name(database_name)
        self.full_name = CBASHelper.format_name(
            self.database_name, self.name)

        self.datasets = dict()
        self.remote_datasets = dict()
        self.external_datasets = dict()
        self.standalone_datasets = dict()

        self.synonyms = dict()
        self.udfs = dict()

    def __str__(self):
        return self.name


class Link(object):
    """
    Link object
    """

    def __init__(self, name):
        """
        :param name str, name of the link, not needed in case of a link of
        type Local
        :param dataverse_name str, dataverse where the link is present.
        """
        self.name = CBASHelper.format_name(name)

    def __str__(self):
        return self.name


class Remote_Link(Link):

    def __init__(self, name, properties={}):
        """
        :param name str, name of the link

        <Required> hostname : The hostname of the link
        <Optional> username : The username for host authentication.
        Required if encryption is set to "none" or "half. Optional if
        encryption is set to "full".
        <Optional> password : The password for host authentication.
        Required if encryption is set to "none" or "half. Optional if
        encryption is set to "full".
        <Required> encryption : The link secure connection type
        ('none', 'full' or 'half')
        <Optional> certificate : The root certificate of target cluster for authentication.
        Required only if encryption is set to "full"
        <Optional> clientCertificate : The user certificate for authentication.
        Required only if encryption is set to "full" and username and password is not used.
        <Optional> clientKey : The client key for user authentication.
        Required only if encryption is set to "full" and username and password is not used.
        """
        super(Remote_Link, self).__init__(name)
        self.properties = properties
        self.properties["name"] = CBASHelper.unformat_name(self.name)
        self.link_type = "couchbase"


class External_Link(Link):

    def __init__(self, name,  properties={}):
        """
        :param name str, name of the link

        <Required> accessKeyId : The access key of the link
        <Required> secretAccessKey : The secret key of the link
        <Required> region : The region of the link
        <Optional> serviceEndpoint : The service endpoint of the link.
        Note - please use the exact key names as provided above in link properties dict.
        """
        super(External_Link, self).__init__(name)
        self.properties = properties
        self.properties["name"] = CBASHelper.unformat_name(self.name)
        self.link_type = self.properties["type"].lower()


class Kafka_Link(Link):

    def __init__(self, name, kafka_type="confluent",
                 kafka_cluster_details={}, schema_registry_details={}):
        """
        :param name str, name of the link
        :param kafka_type <str> Type of kafka cluster. Accepted values
        are confluent and aws_msk
        :param kafka_cluster_details <dict> kafka cluster info like
        connection URI and authentication.
        :param schema_registry_details <dict> info to connect to schema
        registry
        """
        super(Kafka_Link, self).__init__(name)
        self.link_type = "kafka"
        self.kafka_type = kafka_type
        self.kafka_cluster_details = kafka_cluster_details
        self.schema_registry_details = schema_registry_details


class Dataset(object):

    def __init__(self, name, dataverse_name="Dafault", database_name="Default",
                 bucket=None, scope=None, collection=None,
                 num_of_items=0, storage_format="row"):
        """
        :param name <str> name of the dataset
        :param dataverse_name <str> dataverse where the dataset is present.
        :param bucket <bucket_obj> KV bucket on which dataset is based.
        :param scope <str> KV scope on which dataset is based.
        If only bucket name is specified, then default scope is selected.
        :param collection <str> KV collection on which dataset is based.
        If only bucket name is specified, then default collection in default scope is selected.
        :param num_of_items <int> expected number of items in dataset.
        :param storage_format <str> storage format for the dataset.
        """
        self.name = CBASHelper.format_name(name)
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.database_name = CBASHelper.format_name(database_name)
        self.full_name = CBASHelper.format_name(
            self.database_name, self.dataverse_name, self.name)
        self.indexes = dict()
        self.kv_bucket = bucket
        self.kv_scope = scope
        self.kv_collection = collection
        if isinstance(self.kv_bucket, str):
            self.full_kv_entity_name = CBASHelper.format_name(
                self.kv_bucket, self.kv_scope, self.kv_collection)
        else:
            if self.kv_collection:
                self.full_kv_entity_name = (
                    self.get_fully_qualified_kv_entity_name(cardinality=3))
            elif self.kv_bucket:
                self.full_kv_entity_name = (
                    self.get_fully_qualified_kv_entity_name(cardinality=1))
            else:
                self.full_kv_entity_name = None
        self.num_of_items = num_of_items
        self.storage_format = storage_format

    def __str__(self):
        return self.full_name

    def get_fully_qualified_kv_entity_name(self, cardinality=1):
        if cardinality == 1:
            return CBASHelper.format_name(self.kv_bucket.name)
        elif cardinality == 2:
            return CBASHelper.format_name(self.kv_bucket.name,
                                          self.kv_scope.name)
        elif cardinality == 3:
            return CBASHelper.format_name(self.kv_bucket.name,
                                          self.kv_scope.name,
                                          self.kv_collection.name)

    def reset_full_name(self):
        self.full_name = CBASHelper.format_name(
            self.database_name, self.dataverse_name, self.name)


class Remote_Dataset(Dataset):

    def __init__(self, name, link_name, dataverse_name="Dafault",
                 database_name="Default", bucket=None, scope=None,
                 collection=None, num_of_items=0, storage_format="row",
                 capella_as_source=False):
        """
        :param name <str> name of the dataset
        :param link_name <str> name of the remote link to which dataset is
        associated.
        :param dataverse_name <str> dataverse where the dataset is present.
        :param bucket <bucket_obj> KV bucket on which dataset is based.
        :param scope <str> KV scope on which dataset is based.
        If only bucket name is specified, then default scope is selected.
        :param collection <str> KV collection on which dataset is based.
        If only bucket name is specified, then default collection in
        default scope is selected.
        :param num_of_items <int> expected number of items in dataset.
        :param storage_format <str> storage format for the dataset.
        :param capella_as_source <bool> True if source for remote dataset is
        capella cluster else False if source is on-prem cluster
        """
        super(Remote_Dataset, self).__init__(
            name, dataverse_name, database_name, bucket, scope,
            collection, num_of_items, storage_format)

        self.capella_as_source = capella_as_source
        self.link_name = CBASHelper.format_name(link_name)


class External_Dataset(Dataset):

    def __init__(self, name, link_name, dataverse_name="Dafault",
                 database_name="Default", dataset_properties={},
                 num_of_items=0):
        """
        :param name <str> name of the dataset
        :param dataverse_name <str> dataverse where the dataset is present.
        :param link_name <str> name of the link to which dataset is associated,
        :param dataset_properties <dict> valid only for dataset with
        dataset_source as external
        :param num_of_items <int> expected number of items in dataset.
        """
        super(External_Dataset, self).__init__(
            name, dataverse_name, database_name, None, None, None,
            num_of_items, "")
        self.link_name = CBASHelper.format_name(link_name)
        self.dataset_properties = dataset_properties


class Standalone_Dataset(Dataset):

    def __init__(self, name, data_source, primary_key,
                 dataverse_name="Dafault", database_name="Default",
                 link_name=None, external_collection_name=None,
                 dataset_properties={}, num_of_items=0, storage_format="row"):
        """
        :param name <str> name of the dataset
        :param primary_key <dict> dict of field_name:field_type to be used
        as primary key.
        :param dataverse_name <str> dataverse where the dataset is present.
        :param data_source <str> Source from where data will be ingested
        into dataset. Accepted Values - mongo, dynamo, cassandra, s3, gcp,
        azure, shadow_dataset, crud(if standalone collection is used only for
        insert, upsert and delete)
        :param link_name <str> Fully qualified name of the kafka link.
        :param external_collection <str> Fully qualified name of the
        collection on external databases like mongo, dynamo, cassandra etc
        :param dataset_properties <dict> valid only for dataset with
        dataset_source as external
        :param num_of_items <int> expected number of items in dataset.
        :param storage_format <str> storage format for the dataset.
        """
        super(Standalone_Dataset, self).__init__(
            name, dataverse_name, database_name, None, None, None,
            num_of_items, storage_format)

        self.primary_key = primary_key
        self.link_name = CBASHelper.format_name(link_name)
        self.data_source = data_source.lower() if data_source else None

        if self.data_source in ["s3", "azure", "gcp"]:
            self.dataset_properties = dataset_properties
        else:
            self.dataset_properties = {}
            if self.data_source in ["mongo", "dynamo", "rds"]:
                self.external_collection_name = external_collection_name


class Synonym(object):
    """
    Analytics synonym object
    """

    def __init__(self, name, cbas_entity_name, cbas_entity_dataverse,
                 cbas_entity_database, dataverse_name="Default",
                 database_name="Default", synonym_on_synonym=False):
        """
        :param name str, name of the synonym
        :param cbas_entity_name str, Cbas entity on which Synonym is based,
        can be Dataset/CBAS_collection/Synonym name.
        :param cbas_entity_dataverse str, dataverse name where the cbas_entity is present.
        :param dataverse str dataverse where the synonym is present.
        :param synonym_on_synonym bool, True if synonym was created on another synonym.
        """
        self.name = CBASHelper.format_name(name)
        self.cbas_entity_name = CBASHelper.format_name(cbas_entity_name)
        self.cbas_entity_dataverse = CBASHelper.format_name(
            cbas_entity_dataverse)
        self.cbas_entity_database = CBASHelper.format_name(
            cbas_entity_database)
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.database_name = CBASHelper.format_name(database_name)
        self.full_name = CBASHelper.format_name(
            self.database_name, self.dataverse_name, self.name)
        self.cbas_entity_full_name = CBASHelper.format_name(
            self.cbas_entity_database, self.cbas_entity_dataverse,
            self.cbas_entity_name)
        self.synonym_on_synonym = synonym_on_synonym

    def __str__(self):
        return self.full_name


class CBAS_Index(object):
    """
    Analytics index object
    """

    def __init__(self, name, dataset_name, dataverse_name, database_name,
                 indexed_fields=None):
        """
        :param name str, name of the index
        :param dataset_name str, dataset/analytics_collection on which index is created
        :param dataverse_name str, name of the dataverse where the dataset is present.
        :param indexed_fields str fields on which index is created,
        format "field_name_1:field_type_1-field_name_2:field_type_2"
        """
        self.name = CBASHelper.format_name(name)
        self.dataset_name = CBASHelper.format_name(dataset_name)
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.database_name = CBASHelper.format_name(database_name)
        self.full_dataset_name = CBASHelper.format_name(
            self.database_name, self.dataverse_name, self.dataset_name)
        self.analytics_index = False
        self.indexed_fields = []
        if indexed_fields:
            self.indexed_fields = indexed_fields.split("-")

    def __str__(self):
        return self.name


class CBAS_UDF(object):
    """
    Analytics UDF object
    """

    def __init__(self, name, dataverse_name, database_name, parameters, body,
                 referenced_entities):
        """
        :param name str, name of the User defined fucntion
        :param dataverse str, name of the dataverse where the UDF is
        present.
        :param parameters list parameters used while creating the UDF.
        :param body str function body
        :param referenced_entities list list of datasets or UDF referenced in
        the function body
        """
        self.name = CBASHelper.format_name(name)
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.database_name = CBASHelper.format_name(database_name)
        self.parameters = parameters
        if parameters and parameters[0] == "...":
            self.arity = -1
        else:
            self.arity = len(parameters)
        self.body = body
        self.dataset_dependencies = list()
        self.udf_dependencies = list()
        self.synonym_dependencies = list()
        for entity in referenced_entities:
            if isinstance(entity, Dataset):
                self.dataset_dependencies.append([
                    CBASHelper.unformat_name(entity.dataverse_name),
                    CBASHelper.unformat_name(entity.name)])
            elif isinstance(entity, Synonym):
                self.synonym_dependencies.append([
                    CBASHelper.unformat_name(entity.dataverse_name),
                    CBASHelper.unformat_name(entity.name)])
            elif isinstance(entity, CBAS_UDF):
                self.udf_dependencies.append([
                    CBASHelper.unformat_name(entity.dataverse_name),
                    CBASHelper.unformat_name(entity.name),
                    entity.arity
                ])
        self.full_name = CBASHelper.format_name(self.dataverse_name, self.name)

    def __str__(self):
        return self.name

    def reset_full_name(self):
        self.full_name = CBASHelper.format_name(self.dataverse_name, self.name)


class KafkaClusterDetails(object):

    # Auth Types
    CONFLUENT_AUTH_TYPES = ["PLAIN", "OAUTH", "SCRAM_SHA_256",
                            "SCRAM_SHA_512"],
    AWS_AUTH_TYPES = ["SCRAM_SHA_512", "IAM"]

    # Encryption Types
    ENCRYPTION_TYPES = ["PLAINTEXT", "TLS"]

    KAFKA_CLUSTER_DETAILS_TEMPLATE = {
        "vendor": None,
        "brokersUrl": None,  # Comma separated list of brokers
        "authenticationDetails": {
            "authenticationType": None,
            "encryptionType": None,
            "credentials": {}
        }
    }

    # Use this if authenticationType is PLAIN,SCRAM_SHA_256,SCRAM_SHA_512
    CONFLUENT_CREDENTIALS = {
        "apiKey": None,
        "apiSecret": None
    }

    # Use this if authenticationType is OAUTH
    CONFLUENT_CREDENTIALS_OAUTH = {
        "oauthTokenEndpointURL": None,
        "clientId": None,
        "clientSecret": None,
        "scope": None,  # optional
        "extension_logicalCluster": None,  # optional
        "extension_identityPoolId": None  # optional
    }

    # Use this if authenticationType is SCRAM_SHA_512
    AWS_MSK_CREDENTIALS = {
        "username": None,
        "password": None
    }

    CONFLUENT_SCHEMA_REGISTRY_DETAILS_TEMPLATE = {
        "connectionFields": {
            "schemaRegistryURL": None,
            "schemaRegistryCredentials": None  # Format "Api Key:Api Secret"
        }
    }

    AWS_MSK_SCHEMA_REGISTRY_DETAILS_TEMPLATE = {
        "connectionFields": {
            "awsRegion": None,
            "accessKeyId": None,
            "secretAccessKey": None,
            "sessionToken": None,
            "registryName": None  # optional
        }
    }

    def __init__(self):
        pass

    def generate_confluent_kafka_cluster_detail(
            self, brokers_url, auth_type, encryption_type, api_key=None,
            api_secret=None, oauthTokenEndpointURL=None, clientId=None,
            clientSecret=None, scope=None, extension_logicalCluster=None,
            extension_identityPoolId=None):
        kafka_cluster_details = deepcopy(self.KAFKA_CLUSTER_DETAILS_TEMPLATE)
        kafka_cluster_details["vendor"] = "CONFLUENT"
        kafka_cluster_details["brokersUrl"] = brokers_url

        auth_type = auth_type.upper()
        if auth_type in self.CONFLUENT_AUTH_TYPES:
            kafka_cluster_details["authenticationDetails"][
                "authenticationType"] = auth_type
            if auth_type == "OAUTH":
                kafka_cluster_details["authenticationDetails"][
                    "credentials"] = deepcopy(self.CONFLUENT_CREDENTIALS_OAUTH)
                kafka_cluster_details["authenticationDetails"][
                    "credentials"][
                    "oauthTokenEndpointURL"] = oauthTokenEndpointURL
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["clientId"] = clientId
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["clientSecret"] = clientSecret
                if scope:
                    kafka_cluster_details["authenticationDetails"][
                        "credentials"]["scope"] = scope
                else:
                    del(kafka_cluster_details["authenticationDetails"][
                        "credentials"]["scope"])
                if extension_logicalCluster:
                    kafka_cluster_details["authenticationDetails"][
                        "credentials"][
                        "extension_logicalCluster"] = extension_logicalCluster
                else:
                    del(kafka_cluster_details["authenticationDetails"][
                        "credentials"]["extension_logicalCluster"])
                if extension_identityPoolId:
                    kafka_cluster_details["authenticationDetails"][
                        "credentials"][
                        "extension_identityPoolId"] = extension_identityPoolId
                else:
                    del(kafka_cluster_details["authenticationDetails"][
                        "credentials"]["extension_identityPoolId"])
            else:
                kafka_cluster_details["authenticationDetails"][
                    "credentials"] = deepcopy(self.CONFLUENT_CREDENTIALS)
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["apiKey"] = api_key
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["apiSecret"] = api_secret
        else:
            raise Exception(
                "Invalid Authentication type. Supported authentication types "
                "are {0}.".format(self.CONFLUENT_AUTH_TYPES))

        if encryption_type.upper() in self.ENCRYPTION_TYPES:
            kafka_cluster_details["authenticationDetails"][
                "encryptionType"] = encryption_type.upper()
        else:
            raise Exception(
                "Invalid Encryption type. Supported Encryption types "
                "are {0}.".format(self.ENCRYPTION_TYPES))
        return KafkaClusterDetails

    def generate_aws_msk_cluster_detail(
            self, brokers_url, auth_type, encryption_type, username=None,
            password=None):
        kafka_cluster_details = deepcopy(self.KAFKA_CLUSTER_DETAILS_TEMPLATE)
        kafka_cluster_details["vendor"] = "AWS_MSK"
        kafka_cluster_details["brokersUrl"] = brokers_url

        auth_type = auth_type.upper()
        if auth_type in self.AWS_AUTH_TYPES:
            kafka_cluster_details["authenticationDetails"][
                "authenticationType"] = auth_type
            if auth_type == "SCRAM_SHA_512":
                kafka_cluster_details["authenticationDetails"][
                    "credentials"] = deepcopy(self.AWS_MSK_CREDENTIALS)
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["username"] = username
                kafka_cluster_details["authenticationDetails"][
                    "credentials"]["password"] = password
        else:
            raise Exception(
                "Invalid Authentication type. Supported authentication types "
                "are {0}.".format(self.AWS_AUTH_TYPES))

        if encryption_type.upper() is "TLS":
            kafka_cluster_details["authenticationDetails"][
                "encryptionType"] = encryption_type.upper()
        else:
            raise Exception(
                "Invalid Encryption type. Supported Encryption types "
                "are {0}.".format("TLS"))
        return KafkaClusterDetails

    def generate_confluent_schema_registry_detail(
            self, schema_registry_url, api_key, api_secret):
        schema_registry_details = deepcopy(
            self.CONFLUENT_SCHEMA_REGISTRY_DETAILS_TEMPLATE)
        schema_registry_details["connectionFields"][
            "schemaRegistryURL"] = schema_registry_url
        schema_registry_details["connectionFields"][
            "schemaRegistryCredentials"] = "{0}:{1}".format(
            api_key, api_secret)
        return schema_registry_details

    def generate_aws_msk_schema_registry_detail(
            self, aws_region, access_key_id, secret_access_key,
            session_token=None, registry_name=None):
        schema_registry_details = deepcopy(
            self.AWS_MSK_SCHEMA_REGISTRY_DETAILS_TEMPLATE)
        schema_registry_details["connectionFields"][
            "awsRegion"] = aws_region
        schema_registry_details["connectionFields"][
            "accessKeyId"] = access_key_id
        schema_registry_details["connectionFields"][
            "secretAccessKey"] = secret_access_key
        if session_token:
            schema_registry_details["connectionFields"][
                "sessionToken"] = session_token
        else:
            del(schema_registry_details["connectionFields"]["sessionToken"])

        if registry_name:
            schema_registry_details["connectionFields"][
                "registryName"] = registry_name
        else:
            del(schema_registry_details["connectionFields"]["registryName"])

        return schema_registry_details


class ExternalDB(object):
    """
    Defines the external DB object.
    """

    """
    :param db_type <str> Source Database can be Mongo, Dynamo
    and Cassandra
    :param mongo_connection_uri <str> Mongo DB connection URI
    :param dynamo_access_key <str> Access key for dynamo service
    :param dynamo_secret_key <str> Secret key for dynamo service
    :param dynamo_region <str> Region in which dynamo table is present
    """

    def __init__(self, db_type, mongo_connection_uri=None,
                 dynamo_access_key=None, dynamo_secret_key=None,
                 dynamo_region=None, rds_hostname=None, rds_username=None,
                 rds_password=None, rds_port=None, rds_server_id=None):
        self.db_type = db_type.lower()
        self.mongo_connection_uri = mongo_connection_uri
        self.dynamo_access_key = dynamo_access_key
        self.dynamo_secret_key = dynamo_secret_key
        self.dynamo_region = dynamo_region
        self.rds_hostname = rds_hostname
        self.rds_username = rds_username
        self.rds_password = rds_password
        self.rds_port = rds_port
        self.rds_server_id = rds_server_id

    def get_source_db_detail_object_for_kafka_links(self):
        if self.db_type == "mongo":
            return {
                "source": "MONGODB",
                "connectionFields": {
                    "connectionUri": self.mongo_connection_uri}
            }
        elif self.db_type == "dynamo":
            return {
                "source": "DYNAMODB",
                "connectionFields": {
                    "accessKeyId": self.dynamo_access_key,
                    "secretAccessKey": self.dynamo_secret_key,
                    "region": self.dynamo_region
                }
            }
        elif self.db_type == "rds":
            return {
                "source": "MYSQLDB",
                "connectionFields": {
                    "databaseHostname": self.rds_hostname,
                    "databasePort": self.rds_port,
                    "databaseUser": self.rds_username,
                    "databasePassword": self.rds_password,
                    "databaseServerId": self.rds_server_id
                }
            }
