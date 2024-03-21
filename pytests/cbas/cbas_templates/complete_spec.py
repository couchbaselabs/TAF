'''
Created on 10-Dec-2020

@author: couchbase
'''
spec = {
    # Accepted values are > 0
    "max_thread_count": 25,

    # Accepted values are 0 or any positive int.
    "no_of_synonyms": 30,

    # Accepted values are 0 or any positive int.
    "no_of_indexes": 25,

    "dataverse": {
        # Accepted values are 0 or any positive int. 0 and 1 means no
        # dataverse will be created and Default dataverse will be used.
        "no_of_dataverses": 25,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are 0,1,2 . 0 means choose a cardinality
        # randomly between 1 or 2
        "cardinality": 0,
        # Accepted values are all or "dataverse" or "analytics_scope"
        "creation_method": "all",
    },

    "remote_link": {
        # Accepted values are 0 or any positive int.
        "no_of_remote_links": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted value is list of property dicts.
        # Dicts should contain link properties for couchbase link
        "properties": [{}],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the link will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the link will not be created.
        "exclude_dataverses": [],
    },

    "external_link": {
        # Accepted values are 0 or any positive int.
        "no_of_external_links": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted value is list of property dicts.
        # Dicts should contain link properties for s3 link
        "properties": [{}],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the link will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the link will not be created.
        "exclude_dataverses": [],
    },

    "kafka_link": {
        # Accepted values are 0 or any positive int.
        "no_of_kafka_links": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are mongo, dynamo, cassandra. Empty list means
        # consider all the database sources.
        "database_type": [],
        # Kafka cluster details
        """
        {
            "CLUSTER_URL": kafka_url,
            "KAFKA_CREDENTIALS": {
                "AUTH_MECHANISM": auth_type,
                "AUTH_FIELDS": {
                    "API_KEY": api_key,
                    "API_SECRET": api_secret
                }
            }
        }
        """
        "kafka_details": [{}],
        # External database details
        """
        { "mongo": [{
                "SOURCE": self.db_type,
                "CONNECTION_URI": self.db_uri
            }],
           "dynamo": [{
                "SOURCE": self.db_type,
                "CONNECTION_URI": self.db_uri
            }]
        }
        """
        "external_database_details": {},
        # Accepted values are list of dataverse names. These are the
        # dataverses where the link will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the link will not be created.
        "exclude_dataverses": [],
    },

    "dataset": {
        "num_of_datasets": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are 0,1,3 . 0 means choose a cardinality
        # randomly between 1 or 3
        "bucket_cardinality": 0,
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will not be created.
        "exclude_dataverses": [],
        # Accepted values are list of bucket names. These are the
        # buckets that will be used while creating a dataset.
        "include_buckets": [],
        # Accepted values are list of bucket names. These are the
        # buckets that will not be used while creating a dataset.
        "exclude_buckets": [],
        # Accepted values are list of scope names. These are the
        # scopes that will be used while creating a dataset.
        "include_scopes": [],
        # Accepted values are list of scope names. These are the
        # scopes that will not be used while creating a dataset.
        "exclude_scopes": [],
        # Accepted values are list of collection names. These are the
        # collections that will be used while creating a dataset.
        "include_collections": [],
        # Accepted values are list of collection names. These are the
        # collections that will not be used while creating a dataset.
        "exclude_collections": [],
        # Accepted values are list of creation methods
        # ["cbas_collection","cbas_dataset","enable_cbas_from_kv"]
        # [] means all methods will be considered while creating dataset.
        "creation_methods": [],
        # This is only for local datasets. Accepted values are -
        # None - Dataset storage will default to row or column based
        # on analytics service level storage format
        # row - Dataset storage will be row format
        # column - Dataset storage will be column format
        # mixed - Dataset storage will be row + column format
        "storage_format": "mixed",
    },

    "remote_dataset": {
        "num_of_remote_datasets": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are 0,1,3 . 0 means choose a cardinality
        # randomly between 1 or 3
        "bucket_cardinality": 0,
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will not be created.
        "exclude_dataverses": [],
        # Accepted values are list of link names. These are the link
        # that will be used while creating a dataset.
        "include_links": [],
        # Accepted values are list of link names. These are the link
        # that will not be used while creating a dataset.
        "exclude_links": [],
        # Accepted values are list of bucket names. These are the
        # buckets that will be used while creating a dataset.
        "include_buckets": [],
        # Accepted values are list of bucket names. These are the
        # buckets that will not be used while creating a dataset.
        "exclude_buckets": [],
        # Accepted values are list of scope names. These are the
        # scopes that will be used while creating a dataset.
        "include_scopes": [],
        # Accepted values are list of scope names. These are the
        # scopes that will not be used while creating a dataset.
        "exclude_scopes": [],
        # Accepted values are list of collection names. These are the
        # collections that will be used while creating a dataset.
        "include_collections": [],
        # Accepted values are list of collection names. These are the
        # collections that will not be used while creating a dataset.
        "exclude_collections": [],
        # Accepted values are list of creation methods
        # ["cbas_collection","cbas_dataset"]
        # [] means all methods will be considered while creating dataset.
        "creation_methods": [],
        # This is only for local datasets. Accepted values are -
        # None - Dataset storage will default to row or column based
        # on analytics service level storage format
        # row - Dataset storage will be row format
        # column - Dataset storage will be column format
        # mixed - Dataset storage will be row + column format
        "storage_format": "mixed",
    },

    "external_dataset": {
        "num_of_external_datasets": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will not be created.
        "exclude_dataverses": [],
        # Accepted values are list of link names. These are the link
        # that will be used while creating a dataset.
        "include_links": [],
        # Accepted values are list of link names. These are the link
        # that will not be used while creating a dataset.
        "exclude_links": [],
        # Accepted values are s3, azure and gcp. If empty it will consider
        # all link types
        "include_link_types": [],
        # Accepted values are s3, azure and gcp. If empty it will consider
        # all link types
        "exclude_link_types": [],
        # This is only applicable while creating external datasets.
        "external_dataset_properties": [{}]
    },

    "standalone_dataset": {
        "num_of_standalone_coll": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will not be created.
        "exclude_dataverses": [],
        # Accepted values are list of creation methods
        # ["Dataset","Analytics collection","collection"]
        # [] means all methods will be considered while creating dataset.
        "creation_methods": [],
        # This is only for standalone collections. This dictionary will be
        # used to  create PK on the dataset
        # {field_name: field_type}
        "primary_key": [{}],
        # This is only for local datasets. Accepted values are -
        # None - Dataset storage will default to row or column based
        # on analytics service level storage format
        # row - Dataset storage will be row format
        # column - Dataset storage will be column format
        # mixed - Dataset storage will be row + column format
        "storage_format": "mixed",
    },

    "kafka_dataset": {
        "num_of_ds_on_external_db": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the dataset will not be created.
        "exclude_dataverses": [],
        # Accepted values are list of link names. These are the link
        # that will be used while creating a dataset.
        "include_links": [],
        # Accepted values are list of link names. These are the link
        # that will not be used while creating a dataset.
        "exclude_links": [],
        # Accepted values are list of creation methods
        # ["Dataset","Analytics collection","collection"]
        # [] means all methods will be considered while creating dataset.
        "creation_methods": [],
        # This is only for standalone collections. This dictionary will be
        # used to  create PK on the dataset
        # {field_name: field_type}
        "primary_key": [{}],
        # Source from where data will be ingested into dataset.
        # Accepted Values - mongo, dynamo, cassandra
        "datasource": [],
        # This is only for local datasets. Accepted values are -
        # None - Dataset storage will default to row or column based
        # on analytics service level storage format
        # row - Dataset storage will be row format
        # column - Dataset storage will be column format
        # mixed - Dataset storage will be row + column format
        "storage_format": "mixed",
    },

    "synonym": {
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of dataverse names. These are the
        # dataverses where the synonym will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the synonym will not be created.
        "exclude_dataverses": [],
        # Accepted values are list of datset names. These are the
        # datsets on which the synonym will be created.
        "include_datasets": [],
        # Accepted values are list of datset names. These are the
        # datsets on which the synonym will not be created.
        "exclude_datasets": [],
        # Accepted values are list of synonym names. These are the
        # synonyms on which the synonym will be created.
        "include_synonyms": [],
        # Accepted values are list of synonym names. These are the
        # synonyms on which the synonym will be created.
        "exclude_synonyms": [],
    },

    "index": {
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of dataverse names. The indexes
        # will only be created on datasets in these dataverses.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. The indexes
        # will not be created on datasets in these dataverses.
        "exclude_dataverses": [],
        # Accepted values are list of datset names. The indexes will
        # only be created on these datasets.
        "include_datasets": [],
        # Accepted values are list of datset names. The indexes will
        # not be created on these datasets.
        "exclude_datasets": [],
        # Accepted values are list of strings. Each string will be
        # treated as one index condition. In order to pass multiple
        # fields to create an index use the following format -
        # "field_name_1:field_type_1-field_name_2:field_type_2"
        "indexed_fields": ["age:bigint"],
        # Accepted values are all, cbas_index or index
        "creation_method": "all"
    },
}
