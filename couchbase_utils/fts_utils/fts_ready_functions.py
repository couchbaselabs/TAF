'''
Created on 07-May-2021

@author: riteshagarwal
'''
from FtsLib.FtsOperations import FtsHelper
from Cb_constants.CBServer import CbServer
from global_vars import logger


class FTSUtils:

    def __init__(self, cluster, server_task):
        self.cluster = cluster
        self.task = server_task
        self.task_manager = self.task_manager
        self.log = logger.get("test")

    def get_fts_param_template(self):
        fts_param_template = {
            "type": "fulltext-index",
            "name": "%s",
            "sourceType": "gocbcore",
            "sourceName": "%s",
            "planParams": {
                "maxPartitionsPerPIndex": 1024,
                "indexPartitions": "%d"
             },
            "params": {
                "doc_config": {
                    "docid_prefix_delim": "",
                    "docid_regexp": "",
                    "mode": "scope.collection.type_field",
                    "type_field": "type"
                    },
                "mapping": {
                    "analysis": {},
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "default_mapping": {
                        "dynamic": "true",
                        "enabled": "false"
                        },
                    "default_type": "_default",
                    "docvalues_dynamic": "false",
                    "index_dynamic": "true",
                    "store_dynamic": "false",
                    "type_field": "_type",
                    "types": {
                        "%s.%s": {
                            "dynamic": "true",
                            "enabled": "true"
                            }
                        }
                    },
                "store": {
                    "indexType": "scorch",
                    "segmentVersion": 15
                    }
                },
            "sourceParams": {}
           }
        return fts_param_template

    def create_fts_indexes(self, buckets, count=100, base_name="fts"):
        """
        Creates count number of fts indexes on collections
        count should be less than number of collections
        """
        self.log.debug("Creating {} fts indexes ".format(count))
        fts_helper = FtsHelper(self.cluster_util.get_nodes_from_services_map(
            service_type=CbServer.Services.FTS,
            get_all_nodes=False))
        couchbase_buckets = [bucket for bucket in buckets if bucket.bucketType=="couchbase"]
        created_count = 0
        fts_indexes = dict()
        for bucket in couchbase_buckets:
            fts_indexes[bucket.name] = dict()
            for _, scope in bucket.scopes.items():
                fts_indexes[bucket.name][scope.name] = dict()
                for _, collection in scope.collections.items():
                    fts_index_name = base_name + str(created_count)
                    fts_param_template = self.get_fts_param_template()
                    status, content = fts_helper.create_fts_index_from_json(
                        fts_index_name,
                        fts_param_template % (fts_index_name,
                                              bucket.name,
                                              self.fts_index_partitions,
                                              scope.name, collection.name))

                    if status is False:
                        self.fail("Failed to create fts index %s: %s"
                                  % (fts_index_name, content))
                    if collection not in fts_indexes[bucket.name][scope.name]:
                        fts_indexes[bucket.name][scope.name][collection.name] = list()
                    fts_indexes[bucket.name][scope.name][collection.name].append(fts_index_name)
                    created_count = created_count + 1
                    if created_count >= count:
                        return fts_indexes

    def drop_fts_indexes(self, fts_dict, count=10):
        """
        Drop count number of fts indexes using fts name
        from fts_dict
        """
        self.log.debug("Dropping {0} fts indexes".format(count))
        fts_helper = FtsHelper(self.cluster_util.get_nodes_from_services_map(
            service_type=CbServer.Services.FTS,
            get_all_nodes=False))
        indexes_dropped = dict()
        dropped_count = 0
        for bucket, bucket_data in fts_dict.items():
            indexes_dropped[bucket] = dict()
            for scope, collection_data in bucket_data.items():
                indexes_dropped[bucket][scope] = dict()
                for collection, fts_index_names in collection_data.items():
                    for fts_index_name in fts_index_names:
                        status, content = fts_helper.delete_fts_index(fts_index_name)
                        if status is False:
                            self.fail("Failed to drop fts index %s: %s"
                                      % (fts_index_name, content))
                        if collection not in indexes_dropped[bucket][scope]:
                            indexes_dropped[bucket][scope][collection] = list()
                        indexes_dropped[bucket][scope][collection].append(fts_index_name)
                        dropped_count = dropped_count + 1
                        if dropped_count >= count:
                            return indexes_dropped