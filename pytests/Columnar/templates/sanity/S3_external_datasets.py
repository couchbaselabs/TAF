"""
Created on 25-Oct-2023

@author: umang
"""

spec = {
    # Accepted values are > 0
    "max_thread_count": 25,

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

    "external_link": {
        # Accepted values are 0 or any positive int.
        "no_of_external_links": 1,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted value is list of property dicts.
        # Dicts should contain link properties for s3 link
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

    "external_dataset": {
        "num_of_external_datasets": 1,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of database names. These are the
        # databases where the link will be created.
        "include_databases": [],
        # Accepted values are list of database names. These are the
        # databases where the link will not be created.
        "exclude_databases": [],
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
        "external_dataset_properties": [{
            "external_container_name": None,
            "object_construction_def": None,
            "path_on_external_container": None,
            "file_format": None,
            "redact_warning": None,
            "header": None,
            "null_string": None,
            "include": None,
            "exclude": None,
            "parse_json_string": 0,
            "convert_decimal_to_double": 0,
            "timezone": "",
            "embed_filter_values": True
        }]
    },

    "synonym": {
        # Accepted values are 0 or any positive int.
        "no_of_synonyms": 1,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of database names. These are the
        # databases where the link will be created.
        "include_databases": [],
        # Accepted values are list of database names. These are the
        # databases where the link will not be created.
        "exclude_databases": [],
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

    # Indexes are not supported on external datasets.
}
