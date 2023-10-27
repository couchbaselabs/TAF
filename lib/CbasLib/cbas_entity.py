"""
Created on 08-Dec-2020

@author: Umang
"""

from CbasLib.CBASOperations import CBASHelper


class Dataverse(object):
    """
    Dataverse object
    """

    def __init__(self, name="Default"):
        self.name = CBASHelper.format_name(name)

        self.remote_links = dict()
        self.external_links = dict()
        self.kafka_links = dict()

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

    def __init__(self, name, dataverse_name="Default"):
        """
        :param name str, name of the link, not needed in case of a link of type Local
        :param dataverse_name str, dataverse where the link is present.
        """
        self.name = CBASHelper.format_name(name)
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.full_name = CBASHelper.format_name(self.dataverse_name, self.name)

    def __str__(self):
        return self.full_name


class Remote_Link(Link):

    def __init__(self, name, dataverse_name="Default", properties={}):
        """
        :param name str, name of the link
        :param dataverse_name str, dataverse where the link is present.

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
        super(Remote_Link, self).__init__(name, dataverse_name)
        self.properties = properties
        self.properties["name"] = CBASHelper.unformat_name(self.name)
        self.properties["dataverse"] = CBASHelper.unformat_name(
            self.dataverse_name)
        self.link_type = "couchbase"


class External_Link(Link):

    def __init__(self, name, dataverse_name="Default", properties={}):
        """
        :param name str, name of the link
        :param dataverse_name str, dataverse where the link is present.

        <Required> accessKeyId : The access key of the link
        <Required> secretAccessKey : The secret key of the link
        <Required> region : The region of the link
        <Optional> serviceEndpoint : The service endpoint of the link.
        Note - please use the exact key names as provided above in link properties dict.
        """
        super(External_Link, self).__init__(name, dataverse_name)
        self.properties = properties
        self.properties["name"] = CBASHelper.unformat_name(self.name)
        self.properties["dataverse"] = CBASHelper.unformat_name(
            self.dataverse_name)
        self.link_type = self.properties["type"].lower()


class Kafka_Link(Link):

    def __init__(self, name, dataverse_name="Default", db_type="mongo",
                 external_database_details={}):
        """
        :param name str, name of the link
        :param dataverse_name str, dataverse where the link is present.
        :param db_type <str> Type of the external database. Accepted values
        are mongo, dynamo and cassandra
        :param kafka_cluster_details <dict> kafka cluster info like
        connection URI and authentication.
        :param external_database_details <dict> connection, authentication
        and other details required to connect to external databases like
        mongo, dynamo or cassandra
        """
        super(Kafka_Link, self).__init__(name, dataverse_name)
        self.link_type = "kafka"
        self.db_type = db_type.lower()
        self.external_database_details = external_database_details


class Dataset(object):

    def __init__(self, name, dataverse_name="Dafault", bucket=None,
                 scope=None, collection=None, enabled_from_KV=False,
                 num_of_items=0, storage_format="row"):
        """
        :param name <str> name of the dataset
        :param dataverse_name <str> dataverse where the dataset is present.
        :param bucket <bucket_obj> KV bucket on which dataset is based.
        :param scope <str> KV scope on which dataset is based.
        If only bucket name is specified, then default scope is selected.
        :param collection <str> KV collection on which dataset is based.
        If only bucket name is specified, then default collection in default scope is selected.
        :param enabled_from_KV <bool> specify whether the dataset was
        created by enabling analytics from KV.
        :param num_of_items <int> expected number of items in dataset.
        :param storage_format <str> storage format for the dataset.
        """
        self.name = CBASHelper.format_name(name)
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.full_name = CBASHelper.format_name(self.dataverse_name, self.name)
        self.indexes = dict()
        self.enabled_from_KV = enabled_from_KV
        self.kv_bucket = bucket
        self.kv_scope = scope
        self.kv_collection = collection
        if self.kv_collection:
            self.full_kv_entity_name = self.get_fully_qualified_kv_entity_name(
                cardinality=3)
        elif self.kv_bucket:
            self.full_kv_entity_name = self.get_fully_qualified_kv_entity_name(
                cardinality=1)
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
        self.full_name = CBASHelper.format_name(self.dataverse_name, self.name)


class Remote_Dataset(Dataset):

    def __init__(self, name, link_name, dataverse_name="Dafault",
                 bucket=None, scope=None, collection=None,
                 num_of_items=0, storage_format="row"):
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
        """
        super(Remote_Dataset, self).__init__(
            name, dataverse_name, bucket, scope, collection, False,
            num_of_items, storage_format)

        self.link_name = CBASHelper.format_name(link_name)


class External_Dataset(Dataset):

    def __init__(self, name, link_name, dataverse_name="Dafault",
                 dataset_properties={}, num_of_items=0):
        """
        :param name <str> name of the dataset
        :param dataverse_name <str> dataverse where the dataset is present.
        :param link_name <str> name of the link to which dataset is associated,
        :param dataset_properties <dict> valid only for dataset with
        dataset_source as external
        :param num_of_items <int> expected number of items in dataset.
        """
        super(External_Dataset, self).__init__(
            name, dataverse_name, None, None, None, False,
            num_of_items, "")
        self.link_name = CBASHelper.format_name(link_name)
        self.dataset_properties = dataset_properties


class Standalone_Dataset(Dataset):

    def __init__(self, name, data_source, primary_key,
                 dataverse_name="Dafault", link_name=None,
                 external_collection_name=None, dataset_properties={},
                 num_of_items=0, storage_format="row"):
        """
        :param name <str> name of the dataset
        :param primary_key <dict> dict of field_name:field_type to be used
        as primary key.
        :param dataverse_name <str> dataverse where the dataset is present.
        :param data_source <str> Source from where data will be ingested
        into dataset. Accepted Values - mongo, dynamo, cassandra, s3, gcp,
        azure, shadow_dataset
        :param link_name <str> Fully qualified name of the kafka link.
        :param external_collection <str> Fully qualified name of the
        collection on external databases like mongo, dynamo, cassandra etc
        :param dataset_properties <dict> valid only for dataset with
        dataset_source as external
        :param num_of_items <int> expected number of items in dataset.
        :param storage_format <str> storage format for the dataset.
        """
        super(Standalone_Dataset, self).__init__(
            name, dataverse_name, None, None, None, False,
            num_of_items, storage_format)

        self.primary_key = primary_key
        self.link_name = CBASHelper.format_name(link_name)
        self.data_source = data_source.lower() if data_source else None

        if self.data_source in ["s3", "azure", "gcp"]:
            self.dataset_properties = dataset_properties
        else:
            self.dataset_properties = {}
            if self.data_source in ["mongo", "dynamo", "cassandra"]:
                self.external_collection_name = external_collection_name


class Synonym(object):
    """
    Analytics synonym object
    """

    def __init__(self, name, cbas_entity_name, cbas_entity_dataverse,
                 dataverse_name="Default",
                 synonym_on_synonym=False):
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
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.full_name = CBASHelper.format_name(self.dataverse_name, self.name)
        self.cbas_entity_full_name = CBASHelper.format_name(
            self.cbas_entity_dataverse, self.cbas_entity_name)
        self.synonym_on_synonym = synonym_on_synonym

    def __str__(self):
        return self.full_name


class CBAS_Index(object):
    """
    Analytics index object
    """

    def __init__(self, name, dataset_name, dataverse_name,
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
        self.full_dataset_name = CBASHelper.format_name(self.dataverse_name,
                                                        self.dataset_name)
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

    def __init__(self, name, dataverse_name, parameters, body,
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
            if isinstance(entity, Dataset) or isinstance(
                entity, CBAS_Collection):
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


class Kafka(object):
    """
    Defines the kafka cluster object.
    """

    """
    :param url <str> Kafka cluster URI.
    :param api_key <str> Kafka API key
    :param api_secret <str> Kafka API secret key
    """
    def __init__(self, url, api_key, api_secret):
        self.kafka_url = url
        self.api_key = api_key
        self.api_secret = api_secret

    def get_kafka_cluster_detail_object_for_kafka_links(
            self, auth_type="PLAIN"):
        return {
            "CLUSTER_URL": self.kafka_url,
            "KAFKA_CREDENTIALS": {
                "AUTH_MECHANISM": auth_type,
                "AUTH_FIELDS": {
                    "API_KEY": self.api_key,
                    "API_SECRET": self.api_secret
                }
            }
        }


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
                 dynamo_region=None):
        self.db_type = db_type.lower()
        self.mongo_connection_uri = mongo_connection_uri
        self.dynamo_access_key = dynamo_access_key
        self.dynamo_secret_key = dynamo_secret_key
        self.dynamo_region = dynamo_region

    def get_source_db_detail_object_for_kafka_links(self):
        if self.db_type == "mongo":
            return {
                "source": "MONGODB",
                "connectionFields": {"connectionUri": self.mongo_connection_uri}
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

