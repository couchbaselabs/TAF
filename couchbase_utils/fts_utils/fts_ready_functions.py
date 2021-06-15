'''
Created on 07-May-2021

@author: riteshagarwal
'''
from FtsLib.FtsOperations import FtsHelper
from Cb_constants.CBServer import CbServer
from global_vars import logger
from TestInput import TestInputSingleton
from BucketLib.bucket import Bucket


class FTSUtils:

    def __init__(self, cluster, cluster_util, server_task):
        self.cluster = cluster
        self.cluster_util = cluster_util
        self.task = server_task
        self.task_manager = self.task.jython_task_manager
        self.input = TestInputSingleton.input
        self.fts_index_partitions = self.input.param("fts_index_partition", 6)
        self.log = logger.get("test")
        self.fts_helper = FtsHelper(self.cluster.fts_nodes[0])

    def get_fts_idx_template(self):
        fts_idx_template = {
            "type": "fulltext-index",
            "name": "fts-index",
            "sourceType": "gocbcore",
            "sourceName": "default",
            "planParams": {
                "maxPartitionsPerPIndex": 1024,
                "indexPartitions": 1
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
                        "dynamic": True,
                        "enabled": False
                        },
                    "default_type": "_default",
                    "docvalues_dynamic": False,
                    "index_dynamic": True,
                    "store_dynamic": False,
                    "type_field": "_type",
                    "types": {}
                    },
                "store": {
                    "indexType": "scorch",
                    "segmentVersion": 15
                    }
                },
            "sourceParams": {}
           }
        return fts_idx_template

    def create_fts_indexes(self, bucket, scope, collection, name="fts_idx"):
        """
        Creates count number of fts indexes on collections
        count should be less than number of collections
        """
        fts_param_template = self.get_fts_idx_template()
        fts_param_template.update({
            "name": name, "sourceName": bucket.name})
        fts_param_template["planParams"].update({
            "indexPartitions": self.fts_index_partitions})
        fts_param_template["params"]["mapping"]["types"].update({
            "%s.%s" % (scope.name, collection.name): {
                "dynamic": True, "enabled": True}
            }
        )
        fts_param_template = str(fts_param_template).replace("True", "true")
        fts_param_template = str(fts_param_template).replace("False", "false")
        fts_param_template = str(fts_param_template).replace("'", "\"")

        self.log.debug("Creating fts index: {}".format(name))
        status, _ = self.fts_helper.create_fts_index_from_json(
            name, str(fts_param_template))
        return status

    def drop_fts_indexes(self, idx_name):
        """
        Drop count number of fts indexes using fts name
        from fts_dict
        """
        self.log.debug("Dropping fts index: {}".format(idx_name))
        status, _ = self.fts_helper.delete_fts_index(idx_name)
        return status
