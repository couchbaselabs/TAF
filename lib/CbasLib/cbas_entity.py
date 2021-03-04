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
        self.links = dict()
        self.datasets = dict()
        self.synonyms = dict()
        self.udfs = dict()

    def __str__(self):
        return self.name


class CBAS_Scope(Dataverse):
    """
    Analytics scope object, this is syntactic same as Dataverse
    """

    def __init__(self, name="Default"):
        super(CBAS_Scope, self).__init__(name)


class Link(object):
    """
    Link object
    """

    def __init__(self, name=None, dataverse_name="Default", properties={}):
        """
        :param name str, name of the link, not needed in case of a link of type Local
        :param dataverse_name str, dataverse where the link is present.
        :param properties: dict, contains all the properties required to create a link.
        Common for both AWS and couchbase link.
        <Required> name : name of the link to be created.
        <Required> scope : name of the dataverse under which the link has to be created.
        <Required> type : s3/couchbase

        For links to external couchbase cluster.
        <Required> hostname : The hostname of the link
        <Optional> username : The username for host authentication. Required if encryption is set to
        "none" or "half. Optional if encryption is set to "full".
        <Optional> password : The password for host authentication. Required if encryption is set to
        "none" or "half. Optional if encryption is set to "full".
        <Required> encryption : The link secure connection type ('none', 'full' or 'half')
        <Optional> certificate : The root certificate of target cluster for authentication.
        Required only if encryption is set to "full"
        <Optional> clientCertificate : The user certificate for authentication.
        Required only if encryption is set to "full" and username and password is not used.
        <Optional> clientKey : The client key for user authentication.
        Required only if encryption is set to "full" and username and password is not used.

        For links to AWS S3
        <Required> accessKeyId : The access key of the link
        <Required> secretAccessKey : The secret key of the link
        <Required> region : The region of the link
        <Optional> serviceEndpoint : The service endpoint of the link.
        Note - please use the exact key names as provided above in link properties dict.
        """
        self.name = CBASHelper.format_name(name)
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.properties = properties
        self.properties["name"] = CBASHelper.unformat_name(self.name)
        self.properties["scope"] = CBASHelper.unformat_name(
            self.dataverse_name)
        self.link_type = self.properties["type"].lower()
        self.full_name = CBASHelper.format_name(self.dataverse_name, self.name)

    def __str__(self):
        return self.full_name


class Dataset(object):
    """
    Dataset object
    """

    def __init__(self, name="cbas_ds", dataverse_name="Dafault",
                 link_name=None, dataset_source="internal",
                 dataset_properties={},
                 bucket=None, scope=None, collection=None,
                 enabled_from_KV=False,
                 num_of_items=0):
        """
        :param name str, name of the dataset
        :param dataverse_name str, dataverse where the dataset is present.
        :param link_name str, name of the link to which dataset is associated,
        required if dataset is being created on remote or external source.
        :param dataset_source str, determines whether the dataset is created on couchbase buckets or
        external data source. Valid values are internal or external.
        :param dataset_properties dict, valid only for dataset with dataset_source as external
        :param bucket bucket_obj KV bucket on which dataset is based.
        :param scope str KV scope on which dataset is based.
        If only bucket name is specified, then default scope is selected.
        :param collection str KV collection on which dataset is based.
        If only bucket name is specified, then default collection in default scope is selected.
        :param enabled_from_KV bool, specify whether the dataset was created by enabling analytics from KV.
        :param num_of_items int, expected number of items in dataset.
        """
        self.name = CBASHelper.format_name(name)
        self.dataverse_name = CBASHelper.format_name(dataverse_name)
        self.full_name = CBASHelper.format_name(self.dataverse_name, self.name)
        self.link_name = CBASHelper.format_name(link_name)
        self.dataset_source = dataset_source
        self.indexes = dict()

        if self.dataset_source == "internal":
            self.dataset_properties = {}
            self.enabled_from_KV = enabled_from_KV
            self.kv_bucket = bucket
            self.kv_scope = scope
            self.kv_collection = collection
            if self.kv_collection:
                self.full_kv_entity_name = self.get_fully_qualified_kv_entity_name(
                    cardinality=3)
            else:
                self.full_kv_entity_name = self.get_fully_qualified_kv_entity_name(
                    cardinality=1)
            self.num_of_items = num_of_items

        elif self.dataset_source == "external":
            self.dataset_properties = dataset_properties
            self.enabled_from_KV = False
            self.kv_bucket = None
            self.kv_scope = None
            self.kv_collection = None
            self.full_kv_entity_name = None
            self.num_of_items = 0

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


class CBAS_Collection(Dataset):
    """
    Analytics collection object, this is syntactic same as Dataset
    """

    def __init__(self, name="cbas_ds", dataverse_name="Dafault",
                 link_name=None, dataset_source="internal",
                 dataset_properties={},
                 bucket=None, scope=None, collection=None,
                 enabled_from_KV=False,
                 num_of_items=0):
        """
        :param name str, name of the dataset
        :param dataverse_name str, dataverse where the dataset is present.
        :param link_name str, name of the link to which dataset is associated,
        required if dataset is being created on remote or external source.
        :param dataset_source str, determines whether the dataset is created on couchbase buckets or
        external data source. Valid values are internal or external.
        :param dataset_properties dict, valid only for dataset with dataset_source as external
        :param bucket bucket_obj KV bucket on which dataset is based.
        :param scope str KV scope on which dataset is based.
        If only bucket name is specified, then default scope is selected.
        :param collection str KV collection on which dataset is based.
        If only bucket name is specified, then default collection in default scope is selected.
        :param enabled_from_KV bool, specify whether the dataset was created by enabling analytics from KV.
        """
        super(CBAS_Collection, self).__init__(
            name, dataverse_name, link_name, dataset_source,
            dataset_properties,
            bucket, scope, collection, enabled_from_KV, num_of_items)


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
        for entity in referenced_entities:
            if isinstance(entity, Dataset) or isinstance(
                entity, CBAS_Collection) or isinstance(
                    entity, Synonym):
                self.dataset_dependencies.append([
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
