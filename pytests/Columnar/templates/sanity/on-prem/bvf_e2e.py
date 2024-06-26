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
            "no_of_databases": 2,
            # Accepted values are random or any string.
            "name_key": "random",
        },

    "dataverse": {
        # Accepted values are 0 or any positive int. 0 and 1 means no
        # dataverse will be created and Default dataverse will be used.
        "no_of_dataverses": 2,
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
        "no_of_remote_links": 2,
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

    "external_link": {
        # Accepted values are 0 or any positive int.
        "no_of_external_links": 2,
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

    "kafka_link": {
        # Accepted values are 0 or any positive int.
        "no_of_kafka_links": 0,
        # Accepted values are random or any string.
        "name_key": "random",
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

        # Accepted values are "mongo", "dynamo", "rds"
        "database_type": [],
        # External database connection details
        # Dict format - {
        # "mongo":[{mongo source details}],
        # "dynamo":[{dynamo source details}] ...
        # }
        "external_database_details": {},
    },

    "remote_dataset": {
        "num_of_remote_datasets": 2,
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
        # Accepted values are list of bucket names. These are the
        # buckets that will be used while creating a dataset.
        "include_buckets": [],
        # Accepted values are list of bucket names. These are the
        # buckets that will not be used while creating a dataset.
        "exclude_buckets": ["copy_to_kv"],
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
        "storage_format": "column",
    },

    "external_dataset": {
        "num_of_external_datasets": 2,
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
        "external_dataset_properties": [
            {
                "external_container_name": None,
                "path_on_external_container": None,
                "file_format": "json",
                "include": ["*.json"],
                "exclude": None,
                "region": None,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            },
            {
                "external_container_name": None,
                "path_on_external_container": None,
                "file_format": "csv",
                "include": ["*.csv"],
                "exclude": None,
                "region": None,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            },
            {
                "external_container_name": None,
                "path_on_external_container": None,
                "file_format": "tsv",
                "include": ["*.tsv"],
                "exclude": None,
                "region": None,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            },
            {
                "external_container_name": None,
                "path_on_external_container": None,
                "file_format": "parquet",
                "include": ["*.parquet"],
                "exclude": None,
                "region": None,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            },
            {
                "external_container_name": None,
                "path_on_external_container": None,
                "file_format": "avro",
                "include": ["*.avro"],
                "exclude": None,
                "region": None,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            }

        ]
    },

    "standalone_dataset": {
        "num_of_standalone_coll": 2,
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
        "primary_key": [{"id": "string"}],
        # This is only for local datasets. Accepted values are -
        # None - Dataset storage will default to row or column based
        # on analytics service level storage format
        # row - Dataset storage will be row format
        # column - Dataset storage will be column format
        # mixed - Dataset storage will be row + column format
        "storage_format": "column",
        # This is to define the data_source as s3, azure, gcp, None(if
        # standalone collection is used only for insert, upsert and delete)
        "data_source": [],
        "standalone_collection_properties": [{}]
    },

    "kafka_dataset": {
        "num_of_ds_on_external_db": 0,
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
        "primary_key": [{"id": "string"}],
        # Source from where data will be ingested into dataset.
        # Accepted Values - mongo, dynamo, cassandra, rds
        "data_source": [],
        # This is only for local datasets. Accepted values are -
        # None - Dataset storage will default to row or column based
        # on analytics service level storage format
        # row - Dataset storage will be row format
        # column - Dataset storage will be column format
        # mixed - Dataset storage will be row + column format
        "storage_format": "mixed",
        # Collections on external database from where the data will be
        # ingested.
        # { "mongo": [],
        #   "dynamo": []
        # }
        "include_kafka_topics": {},
        "exclude_kafka_topics": {},
    },

    "synonym": {
        # Accepted values are 0 or any positive int.
        "no_of_synonyms": 0,
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
        "no_of_indexes": 0,
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
        "indexed_fields": [],
        # Accepted values are all, cbas_index or index
        "creation_method": "all"
    },


}
