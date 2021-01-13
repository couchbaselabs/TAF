'''
Created on 10-Dec-2020

@author: couchbase
'''
spec = {
    # Accepted values are > 0
    "max_thread_count" : 50,
    
    # Accepted values are 0 or any positive int. 0 and 1 means no dataverse will be created and Default dataverse will be used.
    "no_of_dataverses" : 75,
    
    # Accepted values are 0 or any positive int. This is used only to create remote and external link
    "no_of_links" : 100,
    
    # Accepted values are 1 or any positive int.
    "no_of_datasets_per_dataverse" : 3,
    
    # Accepted values are 0 or any positive int.
    "no_of_synonyms" : 200,
    
    # Accepted values are 0 or any positive int.
    "no_of_indexes" : 150,
    
    "dataverse" : {
        # Accepted values are random or any string.
        "name_key" : "random",
        # Accepted values are 0,1,2 . 0 means choose a cardinality randomly between 1 or 2 
        "cardinality" : 0,
        # Accepted values are all or "dataverse" or "analytics_scope"
        "creation_method" : "all",
        },
    
    "link" : {
        # Accepted values are random or any string.
        "name_key" : "random",
        # Accepted value is list of property dicts. Dicts should contain link properties for either
        # s3 or couchbase link
        "properties" : [{}],
        # Accepted values are list of dataverse names. These are the dataverses where the link will be created.
        "include_dataverses" : [],
        # Accepted values are list of dataverse names. These are the dataverses where the link will not be created.
        "exclude_dataverses" : [],
        
        },
    
    "dataset" : {
        # Accepted values are random or any string.
        "name_key" : "random",
        # Accepted values are all, internal or external. 
        "datasource" : "all",
        # Accepted values are 0,1,3 . 0 means choose a cardinality randomly between 1 or 3
        "bucket_cardinality": 0,
        # Accepted values are list of dataverse names. These are the dataverses where the dataset will be created.
        "include_dataverses" : [],
        # Accepted values are list of dataverse names. These are the dataverses where the dataset will not be created.
        "exclude_dataverses" : [],
        # Accepted values are list of link names. These are the link that will be used while creating a dataset.
        "include_links" : [],
        # Accepted values are list of link names. These are the link that will not be used while creating a dataset.
        "exclude_links" : [],
        # Accepted values are all, s3 or couchbase. These are the link type that will be used while creating a dataset.
        "include_link_types" : "all",
        # Accepted values are None, s3 or couchbase. These are the link type that will not be used while creating a dataset.
        "exclude_link_types" : None,
        # Accepted values are list of bucket names. These are the buckets that will be used while creating a dataset.
        "include_buckets" : [],
        # Accepted values are list of bucket names. These are the buckets that will not be used while creating a dataset.
        "exclude_buckets" : [],
        # Accepted values are list of scope names. These are the scopes that will be used while creating a dataset.
        "include_scopes" : [],
        # Accepted values are list of scope names. These are the scopes that will not be used while creating a dataset.
        "exclude_scopes" : [],
        # Accepted values are list of collection names. These are the collections that will be used while creating a dataset.
        "include_collections" : [],
        # Accepted values are list of collection names. These are the collections that will not be used while creating a dataset.
        "exclude_collections" : [],
        # Accepted values are list of creation methods ["cbas_collection","cbas_dataset","enable_cbas_from_kv"].
        # [] means all methods will be considered while creating dataset.
        "creation_methods" : [],
        # This is only applicable while creating external datasets. 
        "external_dataset_properties" : [{}]
        },
    
    "synonym" : {
        # Accepted values are random or any string.
        "name_key" : "random",
        # Accepted values are list of dataverse names. These are the dataverses where the synonym will be created.
        "include_dataverses" : [],
        # Accepted values are list of dataverse names. These are the dataverses where the synonym will not be created.
        "exclude_dataverses" : [],
        # Accepted values are list of datset names. These are the datsets on which the synonym will be created.
        "include_datasets" : [],
        # Accepted values are list of datset names. These are the datsets on which the synonym will not be created.
        "exclude_datasets" : [],
        # Accepted values are list of synonym names. These are the synonyms on which the synonym will be created.
        "include_synonyms" : [],
        # Accepted values are list of synonym names. These are the synonyms on which the synonym will be created.
        "exclude_synonyms" : [],
        },
    
    "index" : {
        # Accepted values are random or any string.
        "name_key" : "random",
        # Accepted values are list of dataverse names. The indexes will only be created on datasets in these dataverses.
        "include_dataverses" : [],
        # Accepted values are list of dataverse names. The indexes will not be created on datasets in these dataverses.
        "exclude_dataverses" : [],
        # Accepted values are list of datset names. The indexes will only be created on these datasets.
        "include_datasets" : [],
        # Accepted values are list of datset names. The indexes will not be created on these datasets.
        "exclude_datasets" : [],
        # Accepted values are list of strings. Each string will be treated as one index condition. In order to pass multiple 
        # fields to create an index use the following format - "field_name_1:field_type_1-field_name_2:field_type_2"
        "indexed_fields" : ["age:bigint"],
        # Accepted values are all, cbas_index or index
        "creation_method" : "all"
        },
    }