'''
Created on 3-January-2024

@author: abhay.aggrawal@couchbase.com
'''
spec = {
    # Accepted values are > 0
    "max_thread_count": 25,

    "database": {
        "no_of_databases": 1
    },

    "dataverse": {
        # Accepted values are 0 or any positive int. 0 and 1 means no
        # dataverse will be created and Default dataverse will be used.
        "no_of_dataverses": 1,
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
        "exclude_scopes": ["_system"],
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
        "primary_key": [{"name": "string", "email": "string"}],
        # This is only for local datasets. Accepted values are -
        # None - Dataset storage will default to row or column based
        # on analytics service level storage format
        # row - Dataset storage will be row format
        # column - Dataset storage will be column format
        # mixed - Dataset storage will be row + column format
        "storage_format": "mixed",
        # This is to define the data_source as shadow_dataset, s3, azure, gcp
        "data_source": [],
        "standalone_collection_properties": [{}]
    },

    "synonym": {
        # Accepted values are 0 or any positive int.
        "no_of_synonyms": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of dataverse names. These are the
        # dataverses where the synonym will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the synonym will not be created.
        "exclude_dataverses": [],
        # Accepted values are list of dataset names. These are the
        # datsets on which the synonym will be created.
        "include_datasets": [],
        # Accepted values are list of dataset names. These are the
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
        # Accepted values are 0 or any positive int.
        "no_of_indexes": 0,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of dataverse names. The indexes
        # will only be created on datasets in these dataverses.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. The indexes
        # will not be created on datasets in these dataverses.
        "exclude_dataverses": [],
        # Accepted values are list of dataset names. The indexes will
        # only be created on these datasets.
        "include_datasets": [],
        # Accepted values are list of dataset names. The indexes will
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

    "api_timeout": 300,

    "cbas_timeout": 300,
}
