'''
Created on 9-March-2024

@author: Umang Agrawal
This is the main template file that contains all the specs that can be
defined for creating analytics entities.
'''
spec = {
    # Accepted values are > 0
    "max_thread_count": 25,
    "api_timeout": 300,
    "cbas_timeout": 300,

    "database": {
            # Accepted values are 0 or any positive int. 0 and 1 means no
            # database will be created and Default database will be used.
            "no_of_databases": 1,
            # Accepted values are random or any string.
            "name_key": "random",
        },

    "dataverse": {
        # Accepted values are 0 or any positive int. 0 and 1 means no
        # dataverse will be created and Default dataverse will be used.
        "no_of_dataverses": 1,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are all or "dataverse" or "analytics_scope" or
        # "scope"
        "creation_method": "all",
        # Accepted values are list of database names. These are the
        # databases where the dataverse will be created.
        "include_databases": [],
        # Accepted values are list of database names. These are the
        # databases where the dataverse will not be created.
        "exclude_databases": []
    },

    "remote_link": {
        # Accepted values are 0 or any positive int.
        "no_of_remote_links": 1,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted value is list of property dicts.
        # Dicts should contain link properties for couchbase link
        "properties": [{}],
        # Accepted values are list of database names. These are the
        # databases where the link will be created.
        "include_databases": [],
        # Accepted values are list of database names. These are the
        # databases where the link will not be created.
        "exclude_databases": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the link will be created.
        "include_dataverses": [],
        # Accepted values are list of dataverse names. These are the
        # dataverses where the link will not be created.
        "exclude_dataverses": [],
    },

    "remote_dataset": {
        "num_of_remote_datasets": 1,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of database names. These are the
        # databases where the link will be created.
        "include_databases": [],
        # Accepted values are list of database names. These are the
        # databases where the link will not be created.
        "exclude_databases": [],
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
        "exclude_scopes": ["_system", "_default"],
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
}
