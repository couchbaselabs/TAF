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

    "standalone_dataset": {
        "num_of_standalone_coll": 1,
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
        # Accepted values are list of creation methods
        # ["Dataset","Analytics collection","collection"]
        # [] means all methods will be considered while creating dataset.
        "creation_methods": [],
        # This is only for standalone collections. This dictionary will be
        # used to  create PK on the dataset
        # {field_name: field_type}
        "primary_key": [
            {},
            {"name": "string", "avgrating": "double"},
            {"name": "string", "price": "double"},
            {"avgrating": "double", "price": "double"},
            {"name": "string", "email": "string"}
        ],
        # This is only for local datasets. Accepted values are -
        # None - Dataset storage will default to row or column based
        # on analytics service level storage format
        # row - Dataset storage will be row format
        # column - Dataset storage will be column format
        # mixed - Dataset storage will be row + column format
        "storage_format": "column",
        # This is to define the data_source as shadow_dataset, s3, azure, gcp
        "data_source": ["s3"],
        "standalone_collection_properties": [{
            "external_container_name": None,
            "object_construction_def": None,
            "path_on_external_container": "",
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

    "index": {
        # Accepted values are 0 or any positive int.
        "no_of_indexes": 3,
        # Accepted values are random or any string.
        "name_key": "random",
        # Accepted values are list of database names. These are the
        # databases where the link will be created.
        "include_databases": [],
        # Accepted values are list of database names. These are the
        # databases where the link will not be created.
        "exclude_databases": [],
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
        "indexed_fields": ["city:string", "phone:string",
                           "country:string-city:string"],
        # Accepted values are all, cbas_index or index
        "creation_method": "all"
    },
}
